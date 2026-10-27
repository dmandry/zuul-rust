// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// In-memory ZooKeeper plus the launcher tree cache, locks, and LauncherApi.
// Lock child names follow kazoo 2.11 (`{uuid}__lock__` / `{uuid}__rlock__`
// plus a 10-digit sequence). The cache winner is the contender whose name
// sorts first on its last 10 characters; the payload is the hostname.

use crate::codec::{decode_polymorphic, decode_zkobject, dumps_compressed, encode_polymorphic};
use crate::jsonutil;
use crate::mmh3;
use crate::model::{
    self, is_allocated_state, is_requested_quota_state, ImageBuildArtifact, ImageUpload,
    NodesetRequest, ProviderNode, QuotaInformation,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Kazoo `EventType` values. `-1` is a session reconnect, which 14.3.0
/// handles by rebuilding the tree cache.
pub const EVENT_SESSION: i32 = -1;
pub const EVENT_CREATED: i32 = 1;
pub const EVENT_DELETED: i32 = 2;
pub const EVENT_CHANGED: i32 = 3;
pub const EVENT_CHILDREN: i32 = 4;

/// Kazoo `AddWatchMode`. Tree caches use persistent recursive.
pub const ADD_WATCH_PERSISTENT: i32 = 0;
pub const ADD_WATCH_PERSISTENT_RECURSIVE: i32 = 1;

/// One ZooKeeper watch notification. Children and data watches may
/// carry the payload from the re-arm read, the way kazoo passes the
/// result of the `get` that set the next watch.
#[derive(Clone, Debug)]
pub struct ZkWatchEvent {
    pub kind: i32,
    pub path: String,
    pub children: Option<Vec<String>>,
    pub data: Option<Vec<u8>>,
    pub exists: bool,
}

#[derive(Clone, Debug)]
pub struct ZkStat {
    pub version: i32,
    pub czxid: i64,
    pub mzxid: i64,
    pub ephemeral_owner: i64,
    pub created: f64,
    pub children_count: i32,
}

#[derive(Clone, Copy, Debug)]
pub enum CreateMode {
    Persistent,
    Ephemeral,
    PersistentSequential,
    EphemeralSequential,
}

impl CreateMode {
    fn ephemeral(self) -> bool {
        matches!(self, CreateMode::Ephemeral | CreateMode::EphemeralSequential)
    }
    fn sequential(self) -> bool {
        matches!(
            self,
            CreateMode::PersistentSequential | CreateMode::EphemeralSequential
        )
    }
}

#[derive(Debug)]
pub enum ZkError {
    NoNode,
    NodeExists,
    NotEmpty,
    BadVersion,
    Message(String),
}

impl std::fmt::Display for ZkError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ZkError::NoNode => write!(f, "no node"),
            ZkError::NodeExists => write!(f, "node exists"),
            ZkError::NotEmpty => write!(f, "not empty"),
            ZkError::BadVersion => write!(f, "bad version"),
            ZkError::Message(m) => write!(f, "{m}"),
        }
    }
}

#[derive(Clone)]
struct ZNode {
    data: Vec<u8>,
    stat: ZkStat,
    /// Next sequence number to append for sequential creates under this node.
    sequence: i64,
}

struct MemoryState {
    nodes: BTreeMap<String, ZNode>,
    zxid: i64,
    session: i64,
    session_alive: bool,
    generation: u64,
    persistent: Vec<String>,
    children_watches: HashSet<String>,
    data_watches: HashSet<String>,
    events: Vec<ZkWatchEvent>,
    waker: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// Operations the launcher needs from a ZooKeeper session. `MemoryZk` is the
/// in-memory test double; the PyO3 harness supplies a TLS client.
pub trait ZkOps: Send + Sync {
    fn session_id(&self) -> i64;
    fn ensure_path(&self, path: &str) -> Result<(), ZkError>;
    fn create(&self, path: &str, data: &[u8], mode: CreateMode) -> Result<String, ZkError>;
    fn delete(&self, path: &str, recursive: bool) -> Result<(), ZkError>;
    fn exists(&self, path: &str) -> Result<Option<ZkStat>, ZkError>;
    fn get(&self, path: &str) -> Result<(Vec<u8>, ZkStat), ZkError>;
    fn set(&self, path: &str, data: &[u8], version: i32) -> Result<ZkStat, ZkError>;
    fn get_children(&self, path: &str) -> Result<Vec<String>, ZkError>;
    /// One round trip for many `get`s. A missing path is `Err(NoNode)`
    /// in that slot. The default issues one `get` per path.
    fn multi_get(&self, paths: &[String]) -> Result<Vec<Result<(Vec<u8>, ZkStat), ZkError>>, ZkError> {
        let mut out = Vec::with_capacity(paths.len());
        for path in paths {
            out.push(self.get(path));
        }
        Ok(out)
    }
    /// One round trip for many `get_children`s. A missing path is
    /// `Err(NoNode)` in that slot.
    fn multi_children(&self, paths: &[String]) -> Result<Vec<Result<Vec<String>, ZkError>>, ZkError> {
        let mut out = Vec::with_capacity(paths.len());
        for path in paths {
            out.push(self.get_children(path));
        }
        Ok(out)
    }
    fn expire_session(&self) {}
    /// Persistent watch. Mode 1 is `PERSISTENT_RECURSIVE`, which is what
    /// `ZuulTreeCache` registers. The default records nothing.
    fn add_watch(&self, _path: &str, _mode: i32) -> Result<(), ZkError> {
        Ok(())
    }
    /// Kazoo `ChildrenWatch`: one `get_children` with the watch bit, then
    /// re-arm after each child event. The default does not watch.
    fn watch_children(&self, path: &str) -> Result<Vec<String>, ZkError> {
        self.get_children(path)
    }
    /// Kazoo `DataWatch`: one `get` with the watch bit. `Ok(None)` is a
    /// missing node, which still arms the watch.
    fn watch_data(&self, path: &str) -> Result<Option<(Vec<u8>, ZkStat)>, ZkError> {
        match self.get(path) {
            Ok(pair) => Ok(Some(pair)),
            Err(ZkError::NoNode) => Ok(None),
            Err(e) => Err(e),
        }
    }
    fn take_watch_events(&self) -> Vec<ZkWatchEvent> {
        Vec::new()
    }
    fn set_waker(&self, _waker: Option<Arc<dyn Fn() + Send + Sync>>) {}
    /// Bumps when the session is replaced. A change means the cache must
    /// be walked again, matching `ZuulTreeCache._start` after reconnect.
    fn session_generation(&self) -> u64 {
        0
    }
}

#[derive(Clone)]
pub struct MemoryZk {
    inner: Arc<Mutex<MemoryState>>,
}

impl MemoryZk {
    pub fn new() -> Self {
        let zk = Self {
            inner: Arc::new(Mutex::new(MemoryState {
                nodes: BTreeMap::new(),
                zxid: 1,
                session: 0x140300,
                session_alive: true,
                generation: 1,
                persistent: Vec::new(),
                children_watches: HashSet::new(),
                data_watches: HashSet::new(),
                events: Vec::new(),
                waker: None,
            })),
        };
        let _ = zk.ensure_path("/zuul");
        zk
    }

    pub fn session_id(&self) -> i64 {
        self.inner.lock().unwrap().session
    }

    pub fn expire_session(&self) {
        let waker = {
            let mut g = self.inner.lock().unwrap();
            g.session_alive = false;
            let session = g.session;
            let doomed: Vec<String> = g
                .nodes
                .iter()
                .filter(|(_, n)| n.stat.ephemeral_owner == session)
                .map(|(p, _)| p.clone())
                .collect();
            for path in doomed {
                g.nodes.remove(&path);
                note_locked(&mut g, EVENT_DELETED, &path, None, None, false);
            }
            g.session_alive = true;
            g.session += 1;
            g.generation = g.generation.wrapping_add(1);
            note_locked(&mut g, EVENT_SESSION, "", None, None, false);
            g.waker.clone()
        };
        if let Some(waker) = waker {
            waker();
        }
    }

    fn now() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }

    pub fn ensure_path(&self, path: &str) -> Result<(), ZkError> {
        let (fired, waker) = {
            let mut g = self.inner.lock().unwrap();
            let mut fired = false;
            let mut acc = String::new();
            for part in path.split('/').filter(|p| !p.is_empty()) {
                acc.push('/');
                acc.push_str(part);
                if !g.nodes.contains_key(&acc) {
                    let zxid = g.zxid;
                    g.zxid += 1;
                    g.nodes.insert(
                        acc.clone(),
                        ZNode {
                            data: Vec::new(),
                            stat: ZkStat {
                                version: 0,
                                czxid: zxid,
                                mzxid: zxid,
                                ephemeral_owner: 0,
                                created: Self::now(),
                                children_count: 0,
                            },
                            sequence: 0,
                        },
                    );
                    fired |= note_locked(&mut g, EVENT_CREATED, &acc, None, None, true);
                    let parent = parent_path(&acc);
                    fired |= note_children(&mut g, &parent);
                }
            }
            (fired, g.waker.clone())
        };
        if fired {
            if let Some(waker) = waker {
                waker();
            }
        }
        Ok(())
    }

    pub fn create(&self, path: &str, data: &[u8], mode: CreateMode) -> Result<String, ZkError> {
        if mode.sequential() {
            let (parent, prefix) = split_parent(path);
            self.ensure_path(&parent)?;
            let mut g = self.inner.lock().unwrap();
            let seq = {
                let parent_node = g.nodes.get_mut(&parent).ok_or(ZkError::NoNode)?;
                let seq = parent_node.sequence;
                parent_node.sequence += 1;
                seq
            };
            let child = format!("{prefix}{seq:010}");
            let full = format!("{parent}/{child}");
            if g.nodes.contains_key(&full) {
                return Err(ZkError::NodeExists);
            }
            let zxid = g.zxid;
            g.zxid += 1;
            let owner = if mode.ephemeral() { g.session } else { 0 };
            g.nodes.insert(
                full.clone(),
                ZNode {
                    data: data.to_vec(),
                    stat: ZkStat {
                        version: 0,
                        czxid: zxid,
                        mzxid: zxid,
                        ephemeral_owner: owner,
                        created: Self::now(),
                        children_count: 0,
                    },
                    sequence: 0,
                },
            );
            let fired = note_locked(&mut g, EVENT_CREATED, &full, None, None, true)
                | note_children(&mut g, &parent);
            let waker = g.waker.clone();
            drop(g);
            if fired {
                if let Some(waker) = waker {
                    waker();
                }
            }
            return Ok(full);
        }
        let mut g = self.inner.lock().unwrap();
        if g.nodes.contains_key(path) {
            return Err(ZkError::NodeExists);
        }
        let parent = parent_path(path);
        if parent != "/" && !parent.is_empty() && !g.nodes.contains_key(&parent) {
            return Err(ZkError::NoNode);
        }
        let zxid = g.zxid;
        g.zxid += 1;
        let owner = if mode.ephemeral() { g.session } else { 0 };
        g.nodes.insert(
            path.to_string(),
            ZNode {
                data: data.to_vec(),
                stat: ZkStat {
                    version: 0,
                    czxid: zxid,
                    mzxid: zxid,
                    ephemeral_owner: owner,
                    created: Self::now(),
                    children_count: 0,
                },
                sequence: 0,
            },
        );
        let fired = note_locked(&mut g, EVENT_CREATED, path, None, None, true)
            | note_children(&mut g, &parent);
        let waker = g.waker.clone();
        drop(g);
        if fired {
            if let Some(waker) = waker {
                waker();
            }
        }
        Ok(path.to_string())
    }

    pub fn delete(&self, path: &str, recursive: bool) -> Result<(), ZkError> {
        let mut g = self.inner.lock().unwrap();
        if !g.nodes.contains_key(path) {
            return Err(ZkError::NoNode);
        }
        let prefix = format!("{path}/");
        let children: Vec<String> = g
            .nodes
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        if !children.is_empty() && !recursive {
            return Err(ZkError::NotEmpty);
        }
        let mut fired = false;
        for child in children {
            g.nodes.remove(&child);
            fired |= note_locked(&mut g, EVENT_DELETED, &child, None, None, false);
        }
        g.nodes.remove(path);
        fired |= note_locked(&mut g, EVENT_DELETED, path, None, None, false);
        fired |= note_children(&mut g, &parent_path(path));
        let waker = g.waker.clone();
        drop(g);
        if fired {
            if let Some(waker) = waker {
                waker();
            }
        }
        Ok(())
    }

    pub fn exists(&self, path: &str) -> Result<Option<ZkStat>, ZkError> {
        let g = self.inner.lock().unwrap();
        Ok(g.nodes.get(path).map(|n| {
            let mut stat = n.stat.clone();
            let prefix = format!("{path}/");
            stat.children_count = g.nodes.keys().filter(|k| k.starts_with(&prefix) && !k[prefix.len()..].contains('/')).count() as i32;
            stat
        }))
    }

    pub fn get(&self, path: &str) -> Result<(Vec<u8>, ZkStat), ZkError> {
        let g = self.inner.lock().unwrap();
        let n = g.nodes.get(path).ok_or(ZkError::NoNode)?;
        Ok((n.data.clone(), n.stat.clone()))
    }

    pub fn set(&self, path: &str, data: &[u8], version: i32) -> Result<ZkStat, ZkError> {
        let mut g = self.inner.lock().unwrap();
        let zxid = g.zxid;
        g.zxid += 1;
        let n = g.nodes.get_mut(path).ok_or(ZkError::NoNode)?;
        if version >= 0 && n.stat.version != version {
            return Err(ZkError::BadVersion);
        }
        n.data = data.to_vec();
        n.stat.version += 1;
        n.stat.mzxid = zxid;
        let stat = n.stat.clone();
        let fired = note_locked(&mut g, EVENT_CHANGED, path, None, None, true);
        let waker = g.waker.clone();
        drop(g);
        if fired {
            if let Some(waker) = waker {
                waker();
            }
        }
        Ok(stat)
    }

    pub fn get_children(&self, path: &str) -> Result<Vec<String>, ZkError> {
        let g = self.inner.lock().unwrap();
        if !g.nodes.contains_key(path) {
            return Err(ZkError::NoNode);
        }
        let prefix = format!("{path}/");
        let mut kids = Vec::new();
        for key in g.nodes.keys() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                if !rest.is_empty() && !rest.contains('/') {
                    kids.push(rest.to_string());
                }
            }
        }
        kids.sort();
        Ok(kids)
    }
}

impl ZkOps for MemoryZk {
    fn session_id(&self) -> i64 {
        MemoryZk::session_id(self)
    }
    fn ensure_path(&self, path: &str) -> Result<(), ZkError> {
        MemoryZk::ensure_path(self, path)
    }
    fn create(&self, path: &str, data: &[u8], mode: CreateMode) -> Result<String, ZkError> {
        MemoryZk::create(self, path, data, mode)
    }
    fn delete(&self, path: &str, recursive: bool) -> Result<(), ZkError> {
        MemoryZk::delete(self, path, recursive)
    }
    fn exists(&self, path: &str) -> Result<Option<ZkStat>, ZkError> {
        MemoryZk::exists(self, path)
    }
    fn get(&self, path: &str) -> Result<(Vec<u8>, ZkStat), ZkError> {
        MemoryZk::get(self, path)
    }
    fn set(&self, path: &str, data: &[u8], version: i32) -> Result<ZkStat, ZkError> {
        MemoryZk::set(self, path, data, version)
    }
    fn get_children(&self, path: &str) -> Result<Vec<String>, ZkError> {
        MemoryZk::get_children(self, path)
    }
    fn expire_session(&self) {
        MemoryZk::expire_session(self)
    }
    fn add_watch(&self, path: &str, mode: i32) -> Result<(), ZkError> {
        let mut g = self.inner.lock().unwrap();
        if mode == ADD_WATCH_PERSISTENT_RECURSIVE || mode == ADD_WATCH_PERSISTENT {
            if !g.persistent.iter().any(|root| root == path) {
                g.persistent.push(path.to_string());
            }
        }
        Ok(())
    }
    fn watch_children(&self, path: &str) -> Result<Vec<String>, ZkError> {
        let mut g = self.inner.lock().unwrap();
        if !g.nodes.contains_key(path) {
            return Err(ZkError::NoNode);
        }
        g.children_watches.insert(path.to_string());
        Ok(kids_locked(&g, path))
    }
    fn watch_data(&self, path: &str) -> Result<Option<(Vec<u8>, ZkStat)>, ZkError> {
        let mut g = self.inner.lock().unwrap();
        g.data_watches.insert(path.to_string());
        Ok(g.nodes.get(path).map(|n| (n.data.clone(), n.stat.clone())))
    }
    fn take_watch_events(&self) -> Vec<ZkWatchEvent> {
        let mut g = self.inner.lock().unwrap();
        std::mem::take(&mut g.events)
    }
    fn set_waker(&self, waker: Option<Arc<dyn Fn() + Send + Sync>>) {
        self.inner.lock().unwrap().waker = waker;
    }
    fn session_generation(&self) -> u64 {
        self.inner.lock().unwrap().generation
    }
}

fn split_child(rest: &str) -> Option<(&str, Option<&str>)> {
    if rest.is_empty() || rest.contains("//") {
        return None;
    }
    let mut parts = rest.splitn(2, '/');
    let id = parts.next()?;
    if id.is_empty() {
        return None;
    }
    let sub = parts.next().filter(|part| !part.is_empty());
    Some((id, sub))
}

fn covers(root: &str, path: &str) -> bool {
    path == root || path.starts_with(&format!("{root}/"))
}

fn interested(g: &MemoryState, path: &str) -> bool {
    g.persistent.iter().any(|root| covers(root, path)) || g.data_watches.contains(path)
}

fn kids_locked(g: &MemoryState, path: &str) -> Vec<String> {
    let prefix = format!("{path}/");
    let mut kids = Vec::new();
    for key in g.nodes.keys() {
        if let Some(rest) = key.strip_prefix(&prefix) {
            if !rest.is_empty() && !rest.contains('/') {
                kids.push(rest.to_string());
            }
        }
    }
    kids.sort();
    kids
}

fn note_locked(
    g: &mut MemoryState,
    kind: i32,
    path: &str,
    children: Option<Vec<String>>,
    data: Option<Vec<u8>>,
    exists: bool,
) -> bool {
    if kind != EVENT_SESSION && !interested(g, path) {
        return false;
    }
    g.events.push(ZkWatchEvent {
        kind,
        path: path.to_string(),
        children,
        data,
        exists,
    });
    true
}

fn note_children(g: &mut MemoryState, parent: &str) -> bool {
    if parent.is_empty() || !g.children_watches.contains(parent) {
        return false;
    }
    let children = kids_locked(g, parent);
    g.events.push(ZkWatchEvent {
        kind: EVENT_CHILDREN,
        path: parent.to_string(),
        children: Some(children),
        data: None,
        exists: true,
    });
    true
}

fn parent_path(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".into(),
        Some(i) => path[..i].to_string(),
        None => "/".into(),
    }
}

fn split_parent(path: &str) -> (String, String) {
    let parent = parent_path(path);
    let prefix = path[parent.len()..].trim_start_matches('/').to_string();
    (parent, prefix)
}

/// Kazoo exclusive lock (`__lock__`) or read lock (`__rlock__`).
#[derive(Clone, Debug)]
pub struct HeldLock {
    pub path: String,
    pub child: String,
    pub valid: bool,
}

pub fn acquire_lock(
    zk: &dyn ZkOps,
    lock_path: &str,
    identifier: &str,
    read: bool,
) -> Result<Option<HeldLock>, ZkError> {
    zk.ensure_path(lock_path)?;
    // Kazoo prefixes a random uuid. Tests that need a pinned name go through
    // the Python fixture; live locks use the same shape.
    let marker = if read { "__rlock__" } else { "__lock__" };
    let uuid_hex = format!("{:032x}", zk.session_id().unsigned_abs() as u128 ^ random_mix());
    let prefix_path = format!("{lock_path}/{uuid_hex}{marker}");
    let full = zk.create(
        &prefix_path,
        identifier.as_bytes(),
        CreateMode::EphemeralSequential,
    )?;
    let child = full[lock_path.len() + 1..].to_string();
    let won = lock_is_winner(zk, lock_path, &child, read)?;
    if won {
        Ok(Some(HeldLock {
            path: full,
            child,
            valid: true,
        }))
    } else {
        let _ = zk.delete(&full, false);
        Ok(None)
    }
}

fn random_mix() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(1)
}

pub(crate) fn lock_is_winner(
    zk: &dyn ZkOps,
    lock_path: &str,
    child: &str,
    read: bool,
) -> Result<bool, ZkError> {
    let kids = zk.get_children(lock_path)?;
    let Some(mine) = sequence_of(child) else {
        return Ok(false);
    };
    for other in kids {
        if other == child {
            continue;
        }
        let Some(seq) = sequence_of(&other) else { continue };
        if seq >= mine {
            continue;
        }
        // A lower sequence blocks us when it is an exclusive lock, or when
        // we are an exclusive lock and the other is either kind.
        let other_is_writer = other.contains("__lock__");
        let other_is_reader = other.contains("__rlock__");
        if read {
            if other_is_writer {
                return Ok(false);
            }
            let _ = other_is_reader;
        } else if other_is_writer {
            return Ok(false);
        }
    }
    Ok(true)
}

fn sequence_of(child: &str) -> Option<String> {
    if child.len() < 10 {
        return None;
    }
    let tail = &child[child.len() - 10..];
    if tail.chars().all(|c| c.is_ascii_digit()) {
        Some(tail.to_string())
    } else {
        None
    }
}

/// Cache winner: sort contenders by the last 10 characters. Payload is hostname.
pub fn lock_winner(contenders: &BTreeMap<String, Vec<u8>>) -> Option<String> {
    if contenders.is_empty() {
        return None;
    }
    let mut names: Vec<&String> = contenders.keys().collect();
    names.sort_by(|a, b| {
        let ta = last10(a);
        let tb = last10(b);
        ta.cmp(&tb)
    });
    let data = contenders.get(names[0])?;
    if data.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(data).to_string())
    }
}

fn last10(name: &str) -> &str {
    if name.len() <= 10 {
        name
    } else {
        &name[name.len() - 10..]
    }
}

#[derive(Clone, Debug)]
pub struct ComponentInfo {
    pub hostname: String,
    pub state: String,
    pub connection_filter: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct LauncherApi {
    pub zk: Arc<dyn ZkOps>,
    pub hostname: String,
    pub connection_filter: Option<Vec<String>>,
    pub requests: HashMap<String, NodesetRequest>,
    pub nodes: HashMap<String, ProviderNode>,
    pub artifacts: HashMap<String, ImageBuildArtifact>,
    pub uploads: HashMap<String, ImageUpload>,
    pub components: Vec<ComponentInfo>,
    pub quota_used: HashMap<String, QuotaInformation>,
    pub quota_requested: HashMap<String, QuotaInformation>,
    pub provider_label_used: HashMap<(String, String), i64>,
    pub tenant_label_used: HashMap<(String, String), i64>,
    pub ready: bool,
    pub max_zxid: i64,
    /// Contender payloads kept so a lock-child watch updates the holder
    /// with one `get`, the way `LockableZKObjectCache.preCacheHook` does.
    node_locks: HashMap<String, BTreeMap<String, Vec<u8>>>,
    watches_on: bool,
    watched_tenants: HashSet<String>,
    watched_components: HashSet<String>,
    session_gen: u64,
    layout_event: bool,
    image_event: bool,
}

impl LauncherApi {
    pub fn new(zk: Arc<dyn ZkOps>, hostname: &str, filter: Option<Vec<String>>) -> Self {
        let mut api = Self {
            zk,
            hostname: hostname.to_string(),
            connection_filter: filter,
            requests: HashMap::new(),
            nodes: HashMap::new(),
            artifacts: HashMap::new(),
            uploads: HashMap::new(),
            components: Vec::new(),
            quota_used: HashMap::new(),
            quota_requested: HashMap::new(),
            provider_label_used: HashMap::new(),
            tenant_label_used: HashMap::new(),
            ready: false,
            max_zxid: 0,
            node_locks: HashMap::new(),
            watches_on: false,
            watched_tenants: HashSet::new(),
            watched_components: HashSet::new(),
            session_gen: 0,
            layout_event: false,
            image_event: false,
        };
        api.sync();
        api.session_gen = api.zk.session_generation();
        api
    }

    /// Register the same watches 14.3.0's launcher, tree cache, layout
    /// store, and component registry register. Persistent recursive watches
    /// cover the request, node, and image trees. Layout and components use
    /// kazoo `ChildrenWatch` plus a `DataWatch` per child.
    pub fn arm_watches(&mut self) {
        if self.watches_on {
            return;
        }
        for root in [
            "/zuul/nodeset",
            "/zuul/nodes",
            "/zuul/images",
            "/zuul/image-uploads",
        ] {
            let _ = self.zk.ensure_path(root);
            let _ = self.zk.add_watch(root, ADD_WATCH_PERSISTENT_RECURSIVE);
        }
        let _ = self.zk.ensure_path("/zuul/layout");
        if let Ok(tenants) = self.zk.watch_children("/zuul/layout") {
            for tenant in tenants {
                self.watch_tenant(&tenant);
            }
        }
        for kind in [
            "scheduler",
            "executor",
            "merger",
            "fingergw",
            "web",
            "launcher",
        ] {
            let root = format!("/zuul/components/{kind}");
            let _ = self.zk.ensure_path(&root);
            if let Ok(hosts) = self.zk.watch_children(&root) {
                for host in hosts {
                    self.watch_component(kind, &host);
                }
            }
        }
        self.session_gen = self.zk.session_generation();
        self.watches_on = true;
    }

    fn watch_tenant(&mut self, tenant: &str) {
        if !self.watched_tenants.insert(tenant.to_string()) {
            return;
        }
        let path = format!("/zuul/layout/{tenant}");
        let _ = self.zk.watch_data(&path);
    }

    fn watch_component(&mut self, kind: &str, host: &str) {
        let path = format!("/zuul/components/{kind}/{host}");
        if !self.watched_components.insert(path.clone()) {
            return;
        }
        let _ = self.zk.watch_data(&path);
    }

    pub fn take_layout_event(&mut self) -> bool {
        let value = self.layout_event;
        self.layout_event = false;
        value
    }

    pub fn take_image_event(&mut self) -> bool {
        let value = self.image_event;
        self.image_event = false;
        value
    }

    /// Apply watch notifications since the last pass. A quiet pass reads
    /// nothing. A session loss walks the trees once, matching
    /// `ZuulTreeCache._walkTree` after reconnect.
    pub fn prepare(&mut self) {
        if !self.watches_on {
            self.arm_watches();
        }
        let events = self.zk.take_watch_events();
        let generation = self.zk.session_generation();
        let lost = generation != self.session_gen
            || events.iter().any(|event| event.kind == EVENT_SESSION);
        if lost || !self.ready {
            // Arm watches before the walk so a change during the walk is
            // queued, matching ZuulTreeCache._start.
            self.rewatch_after_session();
            self.sync();
            self.session_gen = self.zk.session_generation();
            return;
        }
        if events.is_empty() {
            return;
        }
        let mut nodes_changed = false;
        for event in events {
            if event.kind == EVENT_SESSION {
                continue;
            }
            nodes_changed |= self.apply_event(&event);
        }
        if nodes_changed {
            self.reindex();
        }
    }

    fn rewatch_after_session(&mut self) {
        self.watched_tenants.clear();
        self.watched_components.clear();
        self.watches_on = false;
        self.arm_watches();
    }

    fn apply_event(&mut self, event: &ZkWatchEvent) -> bool {
        let path = event.path.as_str();
        if path == "/zuul/layout" || path.starts_with("/zuul/layout/") {
            self.layout_event = true;
            if event.kind == EVENT_CHILDREN {
                if let Some(tenants) = &event.children {
                    for tenant in tenants {
                        self.watch_tenant(tenant);
                    }
                }
            } else if let Some(tenant) = path.strip_prefix("/zuul/layout/") {
                if !tenant.is_empty() && !tenant.contains('/') {
                    self.watch_tenant(tenant);
                }
            }
            return false;
        }
        if path.starts_with("/zuul/components/") {
            if event.kind == EVENT_CHILDREN {
                let rest = path.trim_start_matches("/zuul/components/");
                if let Some(hosts) = &event.children {
                    for host in hosts {
                        self.watch_component(rest, host);
                    }
                }
            }
            self.load_components();
            return false;
        }
        if path.starts_with("/zuul/images/") || path.starts_with("/zuul/image-uploads/") {
            self.image_event = true;
            self.apply_image_event(event);
            return false;
        }
        self.apply_launcher_event(event)
    }

    pub fn sync(&mut self) {
        self.requests.clear();
        self.nodes.clear();
        self.artifacts.clear();
        self.uploads.clear();
        self.node_locks.clear();
        self.quota_used.clear();
        self.quota_requested.clear();
        self.provider_label_used.clear();
        self.tenant_label_used.clear();
        if let Ok(ids) = self.zk.get_children("/zuul/nodeset/requests") {
            let paths: Vec<String> = ids
                .iter()
                .map(|id| format!("/zuul/nodeset/requests/{id}"))
                .collect();
            let bodies = self.multi_data(&paths);
            let revs: Vec<String> = ids
                .iter()
                .map(|id| format!("/zuul/nodeset/requests/{id}/revision"))
                .collect();
            let rev_data = self.multi_data(&revs);
            let locks: Vec<String> = ids
                .iter()
                .map(|id| format!("/zuul/nodeset/locks/{id}"))
                .collect();
            let lock_kids = self.multi_kids(&locks);
            for (i, id) in ids.into_iter().enumerate() {
                let Some((data, stat)) = bodies.get(i).and_then(|item| item.clone()) else {
                    continue;
                };
                let Ok(val) = decode_zkobject(&data) else {
                    continue;
                };
                let mut req = NodesetRequest::from_value(val);
                req.zk_version = Some(stat.version as i64);
                if let Some(Ok(kids)) = lock_kids.get(i) {
                    req.is_locked = !kids.is_empty();
                }
                if let Some(Some((data, _))) = rev_data.get(i) {
                    if let Ok(rev) = decode_zkobject(data) {
                        req.revision_priority = rev.get("relative_priority").and_then(|v| v.as_i64());
                    }
                }
                self.requests.insert(id, req);
            }
        }
        if let Ok(ids) = self.zk.get_children("/zuul/nodes/nodes") {
            let paths: Vec<String> = ids
                .iter()
                .map(|id| format!("/zuul/nodes/nodes/{id}"))
                .collect();
            let bodies = self.multi_data(&paths);
            let assigns: Vec<String> = ids
                .iter()
                .map(|id| format!("/zuul/nodes/nodes/{id}/assignment"))
                .collect();
            let assign_data = self.multi_data(&assigns);
            let lives: Vec<String> = ids
                .iter()
                .map(|id| format!("/zuul/nodes/nodes/{id}/lifecycle"))
                .collect();
            let life_data = self.multi_data(&lives);
            let locks: Vec<String> = ids
                .iter()
                .map(|id| format!("/zuul/nodes/locks/{id}"))
                .collect();
            let lock_kids = self.multi_kids(&locks);
            let mut child_paths = Vec::new();
            let mut child_index = Vec::new();
            for (i, id) in ids.iter().enumerate() {
                if let Some(Ok(kids)) = lock_kids.get(i) {
                    for child in kids {
                        child_index.push(i);
                        child_paths.push(format!("/zuul/nodes/locks/{id}/{child}"));
                    }
                }
            }
            let child_data = self.multi_data(&child_paths);
            let mut contenders: Vec<BTreeMap<String, Vec<u8>>> = vec![BTreeMap::new(); ids.len()];
            for (slot, path) in child_paths.iter().enumerate() {
                let Some(Some((data, _))) = child_data.get(slot) else {
                    continue;
                };
                let Some(name) = path.rsplit('/').next() else {
                    continue;
                };
                let idx = child_index[slot];
                contenders[idx].insert(name.to_string(), data.clone());
            }
            for (i, id) in ids.iter().enumerate() {
                if !contenders[i].is_empty() {
                    self.node_locks.insert(id.clone(), contenders[i].clone());
                }
            }
            for (i, id) in ids.into_iter().enumerate() {
                let Some((data, stat)) = bodies.get(i).and_then(|item| item.clone()) else {
                    continue;
                };
                let Ok((driver, val)) = decode_polymorphic(&data) else {
                    continue;
                };
                let mut node = ProviderNode::from_parts(driver, val);
                node.zk_version = Some(stat.version as i64);
                if let Some(Ok(_)) = lock_kids.get(i) {
                    let held = &contenders[i];
                    node.is_locked = !held.is_empty();
                    node.lock_holder = lock_winner(held);
                }
                if let Some(Some((data, stat))) = assign_data.get(i) {
                    if let Ok(val) = decode_zkobject(data) {
                        node.assignment_version = Some(stat.version as i64);
                        node.assignment_czxid = Some(stat.czxid);
                        node.request_id = val
                            .get("request_id")
                            .and_then(|v| v.as_str().map(|s| s.to_string()));
                        node.min_request_version =
                            val.get("min_request_version").and_then(|v| v.as_i64());
                        node.tenant_name = val
                            .get("tenant_name")
                            .and_then(|v| v.as_str().map(|s| s.to_string()));
                    }
                }
                if let Some(Some((data, _))) = life_data.get(i) {
                    if let Ok(val) = decode_zkobject(data) {
                        node.next_state = val
                            .get("next_state")
                            .and_then(|v| v.as_str().map(|s| s.to_string()));
                        node.next_comment = val
                            .get("next_comment")
                            .and_then(|v| v.as_str().map(|s| s.to_string()));
                    }
                }
                self.index_node(&id, &node);
                self.nodes.insert(id, node);
                self.max_zxid = self.max_zxid.max(stat.mzxid);
            }
        }
        if let Ok(ids) = self.zk.get_children("/zuul/images/artifacts") {
            let paths: Vec<String> = ids
                .iter()
                .map(|id| format!("/zuul/images/artifacts/{id}"))
                .collect();
            let bodies = self.multi_data(&paths);
            for (id, body) in ids.into_iter().zip(bodies) {
                let Some((data, _)) = body else { continue };
                if let Ok(val) = decode_zkobject(&data) {
                    self.artifacts.insert(id, ImageBuildArtifact::from_value(val));
                }
            }
        }
        if let Ok(ids) = self.zk.get_children("/zuul/image-uploads/uploads") {
            let paths: Vec<String> = ids
                .iter()
                .map(|id| format!("/zuul/image-uploads/uploads/{id}"))
                .collect();
            let bodies = self.multi_data(&paths);
            for (id, body) in ids.into_iter().zip(bodies) {
                let Some((data, _)) = body else { continue };
                if let Ok(val) = decode_zkobject(&data) {
                    self.uploads.insert(id, ImageUpload::from_value(val));
                }
            }
        }
        self.load_components();
        self.ready = true;
    }

    fn apply_image_event(&mut self, event: &ZkWatchEvent) {
        let path = event.path.as_str();
        if let Some(id) = path.strip_prefix("/zuul/images/artifacts/") {
            if id.is_empty() || id.contains('/') {
                return;
            }
            if event.kind == EVENT_DELETED {
                self.artifacts.remove(id);
                return;
            }
            if let Ok((data, _)) = self.zk.get(path) {
                if let Ok(val) = decode_zkobject(&data) {
                    self.artifacts.insert(id.to_string(), ImageBuildArtifact::from_value(val));
                }
            } else {
                self.artifacts.remove(id);
            }
            return;
        }
        if let Some(id) = path.strip_prefix("/zuul/image-uploads/uploads/") {
            if id.is_empty() || id.contains('/') {
                return;
            }
            if event.kind == EVENT_DELETED {
                self.uploads.remove(id);
                return;
            }
            if let Ok((data, _)) = self.zk.get(path) {
                if let Ok(val) = decode_zkobject(&data) {
                    self.uploads.insert(id.to_string(), ImageUpload::from_value(val));
                }
            } else {
                self.uploads.remove(id);
            }
        }
    }

    /// Returns true when the node quota index must be rebuilt.
    fn apply_launcher_event(&mut self, event: &ZkWatchEvent) -> bool {
        let path = event.path.as_str();
        if let Some(rest) = path.strip_prefix("/zuul/nodeset/requests/") {
            return self.apply_request_path(rest, event);
        }
        if let Some(rest) = path.strip_prefix("/zuul/nodeset/locks/") {
            self.apply_request_lock(rest, event);
            return false;
        }
        if let Some(rest) = path.strip_prefix("/zuul/nodes/nodes/") {
            self.apply_node_path(rest, event);
            return true;
        }
        if let Some(rest) = path.strip_prefix("/zuul/nodes/locks/") {
            self.apply_node_lock(rest, event);
            return true;
        }
        false
    }

    fn apply_request_path(&mut self, rest: &str, event: &ZkWatchEvent) -> bool {
        let (id, sub) = match split_child(rest) {
            Some(pair) => pair,
            None => return false,
        };
        if sub.is_none() {
            if event.kind == EVENT_DELETED {
                self.requests.remove(id);
                return false;
            }
            let path = format!("/zuul/nodeset/requests/{id}");
            let Ok((data, stat)) = self.zk.get(&path) else {
                self.requests.remove(id);
                return false;
            };
            let Ok(val) = decode_zkobject(&data) else {
                return false;
            };
            let prev = self.requests.get(id).cloned();
            let mut req = NodesetRequest::from_value(val);
            req.zk_version = Some(stat.version as i64);
            if let Some(prev) = prev {
                req.is_locked = prev.is_locked;
                req.revision_priority = prev.revision_priority;
                req.has_lock = prev.has_lock;
            }
            self.requests.insert(id.to_string(), req);
            return false;
        }
        if sub == Some("revision") {
            if event.kind == EVENT_DELETED {
                if let Some(req) = self.requests.get_mut(id) {
                    req.revision_priority = None;
                }
                return false;
            }
            let path = format!("/zuul/nodeset/requests/{id}/revision");
            if let Ok((data, _)) = self.zk.get(&path) {
                if let Ok(rev) = decode_zkobject(&data) {
                    if let Some(req) = self.requests.get_mut(id) {
                        req.revision_priority = rev.get("relative_priority").and_then(|v| v.as_i64());
                    }
                }
            }
        }
        false
    }

    fn apply_request_lock(&mut self, rest: &str, event: &ZkWatchEvent) {
        let (id, _sub) = match split_child(rest) {
            Some(pair) => pair,
            None => return,
        };
        let path = format!("/zuul/nodeset/locks/{id}");
        let locked = match self.zk.get_children(&path) {
            Ok(kids) => !kids.is_empty(),
            Err(_) => event.kind != EVENT_DELETED,
        };
        if let Some(req) = self.requests.get_mut(id) {
            req.is_locked = locked;
        }
    }

    fn apply_node_path(&mut self, rest: &str, event: &ZkWatchEvent) {
        let (id, sub) = match split_child(rest) {
            Some(pair) => pair,
            None => return,
        };
        if sub.is_none() {
            if event.kind == EVENT_DELETED {
                self.nodes.remove(id);
                self.node_locks.remove(id);
                return;
            }
            let path = format!("/zuul/nodes/nodes/{id}");
            let Ok((data, stat)) = self.zk.get(&path) else {
                self.nodes.remove(id);
                return;
            };
            let Ok((driver, val)) = decode_polymorphic(&data) else {
                return;
            };
            let old = self.nodes.get(id).cloned();
            let mut node = ProviderNode::from_parts(driver, val);
            node.zk_version = Some(stat.version as i64);
            if let Some(old) = old {
                node.is_locked = old.is_locked;
                node.lock_holder = old.lock_holder;
                node.assignment_version = old.assignment_version;
                node.assignment_czxid = old.assignment_czxid;
                node.request_id = old.request_id;
                node.min_request_version = old.min_request_version;
                node.tenant_name = old.tenant_name;
                node.next_state = old.next_state;
                node.next_comment = old.next_comment;
            } else if let Some(held) = self.node_locks.get(id) {
                node.is_locked = !held.is_empty();
                node.lock_holder = lock_winner(held);
            }
            self.max_zxid = self.max_zxid.max(stat.mzxid);
            self.nodes.insert(id.to_string(), node);
            return;
        }
        if sub == Some("assignment") {
            self.apply_assignment(id, event.kind == EVENT_DELETED);
            return;
        }
        if sub == Some("lifecycle") {
            self.apply_lifecycle(id, event.kind == EVENT_DELETED);
        }
    }

    fn apply_assignment(&mut self, id: &str, deleted: bool) {
        let Some(node) = self.nodes.get_mut(id) else {
            return;
        };
        if deleted {
            node.request_id = None;
            node.min_request_version = None;
            node.tenant_name = None;
            node.assignment_version = None;
            node.assignment_czxid = None;
            return;
        }
        let path = format!("/zuul/nodes/nodes/{id}/assignment");
        let Ok((data, stat)) = self.zk.get(&path) else {
            return;
        };
        let Ok(val) = decode_zkobject(&data) else {
            return;
        };
        node.assignment_version = Some(stat.version as i64);
        node.assignment_czxid = Some(stat.czxid);
        node.request_id = val.get("request_id").and_then(|v| v.as_str().map(|s| s.to_string()));
        node.min_request_version = val.get("min_request_version").and_then(|v| v.as_i64());
        node.tenant_name = val.get("tenant_name").and_then(|v| v.as_str().map(|s| s.to_string()));
    }

    fn apply_lifecycle(&mut self, id: &str, deleted: bool) {
        let Some(node) = self.nodes.get_mut(id) else {
            return;
        };
        if deleted {
            node.next_state = None;
            node.next_comment = None;
            return;
        }
        let path = format!("/zuul/nodes/nodes/{id}/lifecycle");
        let Ok((data, _)) = self.zk.get(&path) else {
            return;
        };
        let Ok(val) = decode_zkobject(&data) else {
            return;
        };
        node.next_state = val.get("next_state").and_then(|v| v.as_str().map(|s| s.to_string()));
        node.next_comment = val.get("next_comment").and_then(|v| v.as_str().map(|s| s.to_string()));
    }

    fn apply_node_lock(&mut self, rest: &str, event: &ZkWatchEvent) {
        let (id, sub) = match split_child(rest) {
            Some(pair) => pair,
            None => return,
        };
        let (winner, locked) = {
            let held = self.node_locks.entry(id.to_string()).or_default();
            if let Some(name) = sub {
                if event.kind == EVENT_DELETED {
                    held.remove(name);
                } else {
                    let path = format!("/zuul/nodes/locks/{id}/{name}");
                    if let Ok((data, _)) = self.zk.get(&path) {
                        held.insert(name.to_string(), data);
                    } else {
                        held.remove(name);
                    }
                }
            }
            (lock_winner(held), !held.is_empty())
        };
        if !locked {
            self.node_locks.remove(id);
        }
        if let Some(node) = self.nodes.get_mut(id) {
            node.is_locked = locked;
            node.lock_holder = winner;
        }
    }

    fn multi_data(&self, paths: &[String]) -> Vec<Option<(Vec<u8>, ZkStat)>> {
        if paths.is_empty() {
            return Vec::new();
        }
        match self.zk.multi_get(paths) {
            Ok(items) => items.into_iter().map(|item| item.ok()).collect(),
            Err(_) => paths.iter().map(|path| self.zk.get(path).ok()).collect(),
        }
    }

    fn multi_kids(&self, paths: &[String]) -> Vec<Result<Vec<String>, ZkError>> {
        if paths.is_empty() {
            return Vec::new();
        }
        match self.zk.multi_children(paths) {
            Ok(items) => items,
            Err(_) => paths
                .iter()
                .map(|path| self.zk.get_children(path))
                .collect(),
        }
    }

    /// /zuul/components/launcher/{hostname}{seq} holds uncompressed JSON.
    /// An empty directory leaves any test-injected component list alone.
    fn load_components(&mut self) {
        let Ok(ids) = self.zk.get_children("/zuul/components/launcher") else {
            return;
        };
        if ids.is_empty() {
            return;
        }
        let mut loaded = Vec::new();
        for id in ids {
            let path = format!("/zuul/components/launcher/{id}");
            let Ok((data, _)) = self.zk.get(&path) else {
                continue;
            };
            let Ok(val) = decode_zkobject(&data) else {
                continue;
            };
            let Some(hostname) = val.get("hostname").and_then(|v| v.as_str()) else {
                continue;
            };
            let state = val
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("stopped")
                .to_string();
            let connection_filter = match val.get("connection_filter") {
                Some(Value::Array(items)) => {
                    let names: Vec<String> = items
                        .iter()
                        .filter_map(|item| item.as_str().map(|s| s.to_string()))
                        .collect();
                    if names.is_empty() {
                        None
                    } else {
                        Some(names)
                    }
                }
                _ => None,
            };
            loaded.push(ComponentInfo {
                hostname: hostname.to_string(),
                state,
                connection_filter,
            });
        }
        if !loaded.is_empty() {
            self.components = loaded;
        }
    }

    #[allow(dead_code)]
    fn load_request_side(&self, req: &mut NodesetRequest) {
        let lock_path = format!("/zuul/nodeset/locks/{}", req.uuid());
        if let Ok(kids) = self.zk.get_children(&lock_path) {
            // RequestCache only records whether a lock child exists.
            req.is_locked = !kids.is_empty();
        }
        let rev = format!("/zuul/nodeset/requests/{}/revision", req.uuid());
        if let Ok((data, stat)) = self.zk.get(&rev) {
            if let Ok(val) = decode_zkobject(&data) {
                req.revision_priority = val.get("relative_priority").and_then(|v| v.as_i64());
                let _ = stat;
            }
        }
    }

    #[allow(dead_code)]
    fn load_node_side(&self, node: &mut ProviderNode) {
        let id = node.uuid();
        let lock_path = format!("/zuul/nodes/locks/{id}");
        if let Ok(kids) = self.zk.get_children(&lock_path) {
            let mut contenders = BTreeMap::new();
            for child in kids {
                let path = format!("{lock_path}/{child}");
                if let Ok((data, _)) = self.zk.get(&path) {
                    contenders.insert(child, data);
                }
            }
            node.is_locked = !contenders.is_empty();
            node.lock_holder = lock_winner(&contenders);
        }
        let assign = format!("/zuul/nodes/nodes/{id}/assignment");
        if let Ok((data, stat)) = self.zk.get(&assign) {
            if let Ok(val) = decode_zkobject(&data) {
                node.assignment_version = Some(stat.version as i64);
                node.assignment_czxid = Some(stat.czxid);
                node.request_id = val
                    .get("request_id")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                node.min_request_version = val.get("min_request_version").and_then(|v| v.as_i64());
                node.tenant_name = val
                    .get("tenant_name")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
            }
        }
        let life = format!("/zuul/nodes/nodes/{id}/lifecycle");
        if let Ok((data, _)) = self.zk.get(&life) {
            if let Ok(val) = decode_zkobject(&data) {
                node.next_state = val
                    .get("next_state")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
                node.next_comment = val
                    .get("next_comment")
                    .and_then(|v| v.as_str().map(|s| s.to_string()));
            }
        }
    }

    fn index_node(&mut self, key: &str, node: &ProviderNode) {
        let _ = key;
        if is_allocated_state(&node.state()) {
            let mut q = self
                .quota_used
                .entry(node.provider())
                .or_default()
                .clone();
            q.add(&node.quota());
            self.quota_used.insert(node.provider(), q);
        }
        if is_requested_quota_state(&node.state()) {
            let mut q = self
                .quota_requested
                .entry(node.provider())
                .or_default()
                .clone();
            q.add(&node.quota());
            self.quota_requested.insert(node.provider(), q);
            if let Some(tenant) = &node.tenant_name {
                *self
                    .tenant_label_used
                    .entry((tenant.clone(), node.label()))
                    .or_insert(0) += 1;
            } else if !node.provider().is_empty() {
                *self
                    .provider_label_used
                    .entry((node.provider(), node.label()))
                    .or_insert(0) += 1;
            }
        }
    }

    pub fn reindex(&mut self) {
        let nodes: Vec<(String, ProviderNode)> = self
            .nodes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.quota_used.clear();
        self.quota_requested.clear();
        self.provider_label_used.clear();
        self.tenant_label_used.clear();
        for (k, n) in nodes {
            self.index_node(&k, &n);
        }
    }

    pub fn wait_for_sync(&mut self, _timeout: f64) -> bool {
        if !self.ready {
            self.sync();
        }
        self.ready
    }

    pub fn get_matching_requests(&mut self) -> Vec<String> {
        let mut ids: Vec<String> = self.requests.keys().cloned().collect();
        ids.sort_by(|a, b| {
            let ra = &self.requests[a];
            let rb = &self.requests[b];
            ra.priority()
                .cmp(&rb.priority())
                .then(ra.relative_priority().cmp(&rb.relative_priority()))
                .then(ra.request_time().partial_cmp(&rb.request_time()).unwrap_or(std::cmp::Ordering::Equal))
        });
        let candidates = self.running_launchers(None);
        let mut matched = Vec::new();
        for id in ids {
            let req = self.requests.get_mut(&id).unwrap();
            if req.has_lock {
                matched.push(id);
                continue;
            }
            if req.is_locked {
                continue;
            }
            if req.state() == model::request_state::TEST_HOLD {
                continue;
            }
            let names: Vec<String> = candidates.iter().map(|c| c.hostname.clone()).collect();
            if req.lscores.is_none()
                || names.iter().any(|n| {
                    req.lscores
                        .as_ref()
                        .map(|s| !s.values().any(|h| h == n))
                        .unwrap_or(true)
                })
            {
                let mut scores = BTreeMap::new();
                for n in &names {
                    scores.insert(mmh3::launcher_score(n, &req.hash_key()), n.clone());
                }
                req.lscores = Some(scores);
            }
            let scores = req.lscores.clone().unwrap_or_default();
            let mut ordered: Vec<(u32, String)> = scores.into_iter().collect();
            ordered.sort_by_key(|(s, _)| *s);
            for (_score, name) in ordered {
                let Some(launcher) = candidates.iter().find(|c| c.hostname == name) else {
                    continue;
                };
                if launcher.state != "running" {
                    continue;
                }
                if launcher.hostname == self.hostname {
                    matched.push(id);
                }
                break;
            }
        }
        matched
    }

    pub fn get_matching_nodes(&mut self) -> Vec<String> {
        let mut ids: Vec<String> = self.nodes.keys().cloned().collect();
        ids.sort_by(|a, b| {
            let na = &self.nodes[a];
            let nb = &self.nodes[b];
            na.request_time()
                .partial_cmp(&nb.request_time())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(b))
        });
        let all = self.components.clone();
        let mut matched = Vec::new();
        for id in ids {
            let node = self.nodes.get_mut(&id).unwrap();
            if node.has_lock {
                matched.push(id);
                continue;
            }
            if node.is_locked && node.state() != model::node_state::SNAPSHOT {
                continue;
            }
            let candidates: Vec<ComponentInfo> = all
                .iter()
                .filter(|c| {
                    c.connection_filter.is_none()
                        || c.connection_filter
                            .as_ref()
                            .unwrap()
                            .iter()
                            .any(|n| n == &node.connection_name())
                })
                .cloned()
                .collect();
            let names: Vec<String> = candidates.iter().map(|c| c.hostname.clone()).collect();
            if node.lscores.is_none()
                || names.iter().any(|n| {
                    node.lscores
                        .as_ref()
                        .map(|s| !s.values().any(|h| h == n))
                        .unwrap_or(true)
                })
            {
                let mut scores = BTreeMap::new();
                for n in &names {
                    let key = if node.uuid().is_empty() {
                        id.clone()
                    } else {
                        node.uuid()
                    };
                    scores.insert(mmh3::launcher_score(n, &key), n.clone());
                }
                node.lscores = Some(scores);
            }
            let mut ordered: Vec<(u32, String)> =
                node.lscores.clone().unwrap_or_default().into_iter().collect();
            ordered.sort_by_key(|(s, _)| *s);
            for (_score, name) in ordered {
                let Some(launcher) = candidates.iter().find(|c| c.hostname == name) else {
                    continue;
                };
                if launcher.state != "running" {
                    continue;
                }
                if launcher.hostname == self.hostname {
                    matched.push(id);
                }
                break;
            }
        }
        matched
    }

    fn running_launchers(&self, _filter_conn: Option<&str>) -> Vec<ComponentInfo> {
        if self.components.is_empty() {
            return vec![ComponentInfo {
                hostname: self.hostname.clone(),
                state: "running".into(),
                connection_filter: self.connection_filter.clone(),
            }];
        }
        self.components.clone()
    }

    pub fn launchers(&self) -> Vec<ComponentInfo> {
        self.running_launchers(None)
    }

    pub fn get_quota(&self, provider_cname: &str, include_requested: bool) -> QuotaInformation {
        if include_requested {
            self.quota_requested
                .get(provider_cname)
                .cloned()
                .unwrap_or_default()
        } else {
            self.quota_used
                .get(provider_cname)
                .cloned()
                .unwrap_or_default()
        }
    }

    pub fn node_count(&self, tenant: &str, providers: &[String], label: &str) -> i64 {
        let mut count = *self
            .tenant_label_used
            .get(&(tenant.to_string(), label.to_string()))
            .unwrap_or(&0);
        for p in providers {
            count += *self
                .provider_label_used
                .get(&(p.clone(), label.to_string()))
                .unwrap_or(&0);
        }
        count
    }

    pub fn has_node_expired(&self, node: &ProviderNode, now: f64) -> bool {
        let mut nodes = vec![node.clone()];
        if let Some(main_id) = node.main_node_id() {
            if let Some(main) = self.nodes.get(&main_id) {
                nodes.clear();
                for sub in main.subnodes() {
                    if let Some(subnode) = self.nodes.get(&sub) {
                        nodes.push(subnode.clone());
                    }
                }
            }
        }
        !nodes.is_empty() && nodes.iter().all(|n| n.has_expired(now))
    }

    pub fn write_request(&mut self, req: &NodesetRequest) -> Result<(), ZkError> {
        let path = format!("/zuul/nodeset/requests/{}", req.uuid());
        let bytes = dumps_compressed(&req.serialize());
        self.zk.ensure_path("/zuul/nodeset/requests")?;
        self.zk.ensure_path(&format!("/zuul/nodeset/locks/{}", req.uuid()))?;
        if self.zk.exists(&path)?.is_some() {
            let ver = self.zk.get(&path)?.1.version;
            self.zk.set(&path, &bytes, ver)?;
        } else {
            self.zk.create(&path, &bytes, CreateMode::Persistent)?;
        }
        self.requests.insert(req.uuid(), req.clone());
        Ok(())
    }

    pub fn write_node(&mut self, node: &ProviderNode) -> Result<(), ZkError> {
        let path = format!("/zuul/nodes/nodes/{}", node.uuid());
        let bytes = encode_polymorphic(&node.driver_id, &node.serialize());
        self.zk.ensure_path("/zuul/nodes/nodes")?;
        self.zk.ensure_path(&format!("/zuul/nodes/locks/{}", node.uuid()))?;
        if self.zk.exists(&path)?.is_some() {
            let ver = self.zk.get(&path)?.1.version;
            self.zk.set(&path, &bytes, ver)?;
        } else {
            self.zk.create(&path, &bytes, CreateMode::Persistent)?;
        }
        self.nodes.insert(node.uuid(), node.clone());
        self.reindex();
        Ok(())
    }

    pub fn write_assignment(&mut self, node_id: &str, value: &Value) -> Result<(), ZkError> {
        let path = format!("/zuul/nodes/nodes/{node_id}/assignment");
        if self.zk.exists(&path)?.is_some() {
            return Err(ZkError::NodeExists);
        }
        let bytes = dumps_compressed(value);
        self.zk.create(&path, &bytes, CreateMode::Persistent)?;
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.request_id = value
                .get("request_id")
                .and_then(|v| v.as_str().map(|s| s.to_string()));
            node.min_request_version = value.get("min_request_version").and_then(|v| v.as_i64());
            node.tenant_name = value
                .get("tenant_name")
                .and_then(|v| v.as_str().map(|s| s.to_string()));
            node.assignment_version = Some(0);
            if let Ok((_, stat)) = self.zk.get(&path) {
                node.assignment_czxid = Some(stat.czxid);
                node.assignment_version = Some(stat.version as i64);
            }
        }
        self.reindex();
        Ok(())
    }

    pub fn delete_assignment(&mut self, node_id: &str) -> Result<(), ZkError> {
        let path = format!("/zuul/nodes/nodes/{node_id}/assignment");
        match self.zk.delete(&path, false) {
            Ok(()) | Err(ZkError::NoNode) => {}
            Err(e) => return Err(e),
        }
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.request_id = None;
            node.min_request_version = None;
            node.tenant_name = None;
            node.assignment_version = None;
            node.assignment_czxid = None;
        }
        self.reindex();
        Ok(())
    }

    pub fn delete_node(&mut self, node_id: &str) -> Result<(), ZkError> {
        let path = format!("/zuul/nodes/nodes/{node_id}");
        self.zk.delete(&path, true)?;
        let lock_path = format!("/zuul/nodes/locks/{node_id}");
        let _ = self.zk.delete(&lock_path, true);
        self.nodes.remove(node_id);
        self.reindex();
        Ok(())
    }

    /// Persist an image upload (ImageUpload ZKObject, zlib JSON).
    pub fn write_upload(&mut self, upload: &ImageUpload) -> Result<(), ZkError> {
        let path = format!("/zuul/image-uploads/uploads/{}", upload.uuid());
        let bytes = dumps_compressed(&upload.serialize());
        self.zk.ensure_path("/zuul/image-uploads/uploads")?;
        self.zk
            .ensure_path(&format!("/zuul/image-uploads/locks/{}", upload.uuid()))?;
        if self.zk.exists(&path)?.is_some() {
            let ver = self.zk.get(&path)?.1.version;
            self.zk.set(&path, &bytes, ver)?;
        } else {
            self.zk.create(&path, &bytes, CreateMode::Persistent)?;
        }
        self.uploads.insert(upload.uuid(), upload.clone());
        Ok(())
    }

    /// Delete an image upload znode (and its lock directory).
    pub fn delete_upload(&mut self, upload_uuid: &str) -> Result<(), ZkError> {
        let path = format!("/zuul/image-uploads/uploads/{upload_uuid}");
        match self.zk.delete(&path, false) {
            Ok(()) | Err(ZkError::NoNode) => {}
            Err(e) => return Err(e),
        }
        let lock_path = format!("/zuul/image-uploads/locks/{upload_uuid}");
        let _ = self.zk.delete(&lock_path, true);
        self.uploads.remove(upload_uuid);
        Ok(())
    }
}

pub fn system_id(zk: &dyn ZkOps) -> Result<String, ZkError> {
    match zk.get("/zuul-system") {
        Ok((data, _)) => {
            let v = jsonutil::loads(&String::from_utf8_lossy(&data)).unwrap_or(json!({}));
            Ok(v.get("system_id")
                .and_then(|s| s.as_str())
                .unwrap_or("unknown")
                .to_string())
        }
        Err(ZkError::NoNode) => {
            let id = format!("{:032x}", random_mix());
            let raw = jsonutil::dumps_bytes(&json!({"system_id": id}));
            zk.create("/zuul-system", &raw, CreateMode::Persistent)?;
            Ok(id)
        }
        Err(e) => Err(e),
    }
}

pub fn register_component(
    zk: &dyn ZkOps,
    hostname: &str,
    state: &str,
    filter: &Option<Vec<String>>,
) -> Result<String, ZkError> {
    zk.ensure_path("/zuul/components/launcher")?;
    let bytes = model::component_bytes(hostname, state, filter);
    let prefix = format!("/zuul/components/launcher/{hostname}");
    zk.create(&prefix, &bytes, CreateMode::EphemeralSequential)
}

pub fn set_component(
    zk: &dyn ZkOps,
    path: &str,
    hostname: &str,
    state: &str,
    filter: &Option<Vec<String>>,
) -> Result<(), ZkError> {
    let bytes = model::component_bytes(hostname, state, filter);
    zk.set(path, &bytes, -1)?;
    Ok(())
}
