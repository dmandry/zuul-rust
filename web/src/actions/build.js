// Copyright 2018 Red Hat, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

import * as React from 'react'
import * as API from '../api'

import { fetchLogfile } from './logfile'

import {
  FileIcon,
  FolderIcon,
} from '@patternfly/react-icons'

export const BUILD_FETCH_REQUEST = 'BUILD_FETCH_REQUEST'
export const BUILD_FETCH_SUCCESS = 'BUILD_FETCH_SUCCESS'
export const BUILD_FETCH_FAIL = 'BUILD_FETCH_FAIL'

export const BUILDSET_FETCH_REQUEST = 'BUILDSET_FETCH_REQUEST'
export const BUILDSET_FETCH_SUCCESS = 'BUILDSET_FETCH_SUCCESS'
export const BUILDSET_FETCH_FAIL =    'BUILDSET_FETCH_FAIL'

export const BUILD_OUTPUT_REQUEST = 'BUILD_OUTPUT_FETCH_REQUEST'
export const BUILD_OUTPUT_SUCCESS = 'BUILD_OUTPUT_FETCH_SUCCESS'
export const BUILD_OUTPUT_FAIL = 'BUILD_OUTPUT_FETCH_FAIL'
export const BUILD_OUTPUT_NOT_AVAILABLE = 'BUILD_OUTPUT_NOT_AVAILABLE'

export const BUILD_MANIFEST_REQUEST = 'BUILD_MANIFEST_FETCH_REQUEST'
export const BUILD_MANIFEST_SUCCESS = 'BUILD_MANIFEST_FETCH_SUCCESS'
export const BUILD_MANIFEST_FAIL = 'BUILD_MANIFEST_FETCH_FAIL'
export const BUILD_MANIFEST_NOT_AVAILABLE = 'BUILD_MANIFEST_NOT_AVAILABLE'

export const requestBuild = () => ({
  type: BUILD_FETCH_REQUEST
})

export const receiveBuild = (buildId, build) => ({
  type: BUILD_FETCH_SUCCESS,
  buildId: buildId,
  build: build,
  receivedAt: Date.now()
})

const failedBuild = (buildId, error, url) => {
  error.url = url
  return {
    type: BUILD_FETCH_FAIL,
    buildId,
    error
  }
}

export const requestBuildOutput = () => ({
  type: BUILD_OUTPUT_REQUEST
})

// job-output processing functions
export function renderTree(tenant, build, path, obj, textRenderer, defaultRenderer) {
  const node = {}
  let name = encodeURIComponent(obj.name)

  if ('children' in obj && obj.children) {
    node.children = obj.children.map(
      n => renderTree(tenant, build, path+name+'/', n,
        textRenderer, defaultRenderer))
  }
  if (obj.mimetype === 'application/directory') {
    name = name + '/'
    node.icon = React.createElement(FolderIcon)
  } else {
    node.icon = React.createElement(FileIcon)
  }

  node.id = name
  let log_url = build.log_url
  if (log_url.endsWith('/')) {
    log_url = log_url.slice(0, -1)
  }
  if (obj.mimetype === 'text/plain' ||
      obj.mimetype === 'application/json' ||
      obj.mimetype === 'application/yaml') {
    node.name = textRenderer(tenant, build, path, name, log_url, obj)
  } else {
    node.name = defaultRenderer(log_url, path, name, obj)
  }
  return node
}

export function didTaskFail(task) {
  if (task.failed) {
    return true
  }
  if (task.results) {
    for (let result of task.results) {
      if (didTaskFail(result)) {
        return true
      }
    }
  }
  return false
}

export function hasInterestingKeys (obj, keys) {
  return Object.entries(obj).filter(
    ([k, v]) => (keys.includes(k) && v !== '')
  ).length > 0
}

export function findLoopLabel(item) {
  const label = item._ansible_item_label
  return typeof(label) === 'string' ? label : ''
}

export function shouldIncludeKey(key, value, ignore_underscore, included) {
  if (ignore_underscore && key[0] === '_') {
    return false
  }
  if (included) {
    if (!included.includes(key)) {
      return false
    }
    if (value === '') {
      return false
    }
  }
  return true
}

export function makeTaskPath (path) {
  return path.join('/')
}

export function taskPathMatches (ref, test) {
  if (test.length < ref.length)
    return false
  for (let i=0; i < ref.length; i++) {
    if (ref[i] !== test[i])
      return false
  }
  return true
}


export const receiveBuildOutput = (buildId, output) => {
  const hosts = {}

  const taskFailed = (taskResult) => {
    if (taskResult.rc && taskResult.failed_when_result !== false)
      return true
    else if (taskResult.failed)
      return true
    else
      return false
  }

  // Compute stats
  output.forEach(phase => {
    Object.entries(phase.stats).forEach(([host, stats]) => {
      if (!hosts[host]) {
        hosts[host] = stats
        hosts[host].failed = []
      } else {
        hosts[host].changed += stats.changed
        hosts[host].failures += stats.failures
        hosts[host].ok += stats.ok
      }
      if (stats.failures > 0) {
        // Look for failed tasks
        phase.plays.forEach(play => {
          play.tasks.forEach(task => {
            if (task.hosts[host]) {
              if (task.hosts[host].results &&
                  task.hosts[host].results.length > 0) {
                task.hosts[host].results.forEach(result => {
                  if (taskFailed(result)) {
                    result.name = task.task.name
                    hosts[host].failed.push(result)
                  }
                })
              } else if (taskFailed(task.hosts[host])) {
                let result = task.hosts[host]
                result.name = task.task.name
                hosts[host].failed.push(result)
              }
            }
          })
        })
      }
    })
  })

  // Identify all of the hosttasks (and therefore tasks, plays, and
  // playbooks) which have failed.  The errorIds are either task or
  // play uuids, or the phase+index for the playbook.  Since they are
  // different formats, we can store them in the same set without
  // collisions.
  const errorIds = new Set()
  output.forEach(playbook => {
    playbook.plays.forEach(play => {
      play.tasks.forEach(task => {
        Object.entries(task.hosts).forEach(([, host]) => {
          if (didTaskFail(host)) {
            errorIds.add(task.task.id)
            errorIds.add(play.play.id)
            errorIds.add(playbook.phase + playbook.index)
          }
        })
      })
    })
  })

  return {
    type: BUILD_OUTPUT_SUCCESS,
    buildId: buildId,
    hosts: hosts,
    output: output,
    errorIds: errorIds,
    receivedAt: Date.now()
  }
}

const failedBuildOutput = (buildId, error, url) => {
  error.url = url
  return {
    type: BUILD_OUTPUT_FAIL,
    buildId,
    error
  }
}

export const requestBuildManifest = () => ({
  type: BUILD_MANIFEST_REQUEST
})

export const receiveBuildManifest = (buildId, manifest) => {
  const index = {}

  const renderNode = (root, object) => {
    const path = root + '/' + object.name

    if ('children' in object && object.children) {
      object.children.map(n => renderNode(path, n))
    } else {
      index[path] = object
    }
  }

  manifest.tree.map(n => renderNode('', n))
  return {
    type: BUILD_MANIFEST_SUCCESS,
    buildId: buildId,
    manifest: {tree: manifest.tree, index: index,
      index_links: manifest.index_links},
    receivedAt: Date.now()
  }
}

const failedBuildManifest = (buildId, error, url) => {
  error.url = url
  return {
    type: BUILD_MANIFEST_FAIL,
    buildId,
    error
  }
}

function buildOutputNotAvailable(buildId) {
  return {
    type: BUILD_OUTPUT_NOT_AVAILABLE,
    buildId: buildId,
  }
}

function buildManifestNotAvailable(buildId) {
  return {
    type: BUILD_MANIFEST_NOT_AVAILABLE,
    buildId: buildId,
  }
}

export function fetchBuild(tenant, buildId, state) {
  return async function (dispatch) {
    // Although it feels a little weird to not do anything in an action creator
    // based on the redux state, we do this in here because the function is
    // called from multiple places and it's easier to check for the build in
    // here than in all the other places before calling this function.
    if (state.build.builds[buildId]) {
      return Promise.resolve()
    }

    dispatch(requestBuild())
    try {
      const response = await API.fetchBuild(tenant.apiPrefix, buildId)
      dispatch(receiveBuild(buildId, response.data))
    } catch (error) {
      dispatch(failedBuild(buildId, error, tenant.apiPrefix))
      // Raise the error again, so fetchBuildAllInfo() doesn't call the
      // remaining fetch methods.
      throw error
    }
  }
}

function fetchBuildOutput(buildId, state) {
  return async function (dispatch) {
    // In case the value is already set in our local state, directly resolve the
    // promise. A null value means that the output could not be found for this
    // build id.
    if (state.build.outputs[buildId] !== undefined) {
      return Promise.resolve()
    }
    // As this function is only called after fetchBuild() we can assume that
    // the build is in the state. Otherwise an error would have been thrown and
    // this function wouldn't be called.
    const build = state.build.builds[buildId]
    if (!build.log_url) {
      // Don't treat a missing log URL as failure as we don't want to show a
      // toast for that. The UI already informs about the missing log URL in
      // multiple places.
      return dispatch(buildOutputNotAvailable(buildId))
    }
    const url = build.log_url.substr(0, build.log_url.lastIndexOf('/') + 1)
    dispatch(requestBuildOutput())
    try {
      const auth_log_request = state.info.capabilities?.auth?.auth_log_file_requests === true
      const response = await API.getLogFile(url + 'job-output.json.gz', auth_log_request)
      dispatch(receiveBuildOutput(buildId, response.data))
    } catch (error) {
      if (!error.request) {
        dispatch(failedBuildOutput(buildId, error, url))
        // Raise the error again, so fetchBuildAllInfo() doesn't call the
        // remaining fetch methods.
        throw error
      }
      try {
        // Try without compression
        const auth_log_request = state.info.capabilities?.auth?.auth_log_file_requests === true
        const response = await API.getLogFile(url + 'job-output.json', auth_log_request)
        dispatch(receiveBuildOutput(buildId, response.data))
      } catch (error) {
        dispatch(failedBuildOutput(buildId, error, url))
        // Raise the error again, so fetchBuildAllInfo() doesn't call the
        // remaining fetch methods.
        throw error
      }
    }
  }
}

export function fetchBuildManifest(buildId, state) {
  return async function(dispatch) {
    // In case the value is already set in our local state, directly resolve the
    // promise. A null value means that the manifest could not be found for this
    // build id.
    if (state.build.manifests[buildId] !== undefined) {
      return Promise.resolve()
    }
    // As this function is only called after fetchBuild() we can assume that
    // the build is in the state. Otherwise an error would have been thrown and
    // this function wouldn't be called.
    const build = state.build.builds[buildId]
    dispatch(requestBuildManifest())
    for (let artifact of build.artifacts) {
      if (
        'metadata' in artifact &&
        'type' in artifact.metadata &&
        artifact.metadata.type === 'zuul_manifest'
      ) {
        try {
          const auth_log_request = state.info.capabilities?.auth?.auth_log_file_requests === true
          const response = await API.getLogFile(artifact.url, auth_log_request)
          return dispatch(receiveBuildManifest(buildId, response.data))
        } catch(error) {
          // Show the error since we expected a manifest but did not
          // receive it.
          dispatch(failedBuildManifest(buildId, error, artifact.url))
        }
      }
    }
    // Don't treat a missing manifest file as failure as we don't want to show a
    // toast for that.
    dispatch(buildManifestNotAvailable(buildId))
  }
}

export function fetchBuildAllInfo(tenant, buildId, logfileName) {
  // This wraps the calls to fetch the build, output and manifest together as
  // this is the common use case we have when loading the build info.
  return async function (dispatch, getState) {
    try {
      // Wait for the build to be available as fetchBuildOutput and
      // fetchBuildManifest require information from the build object.
      await dispatch(fetchBuild(tenant, buildId, getState()))
      dispatch(fetchBuildOutput(buildId, getState()))
      // Wait for the manifest info to be available as this is needed in case
      // we also download a logfile.
      await dispatch(fetchBuildManifest(buildId, getState()))
      if (logfileName) {
        dispatch(fetchLogfile(buildId, logfileName, getState()))
      }
    } catch (error) {
      dispatch(failedBuild(buildId, error, tenant.apiPrefix))
    }
  }
}

export const requestBuildset = () => ({
  type: BUILDSET_FETCH_REQUEST
})

export const receiveBuildset = (buildsetId, buildset) => ({
  type: BUILDSET_FETCH_SUCCESS,
  buildsetId: buildsetId,
  buildset: buildset,
  receivedAt: Date.now()
})

const failedBuildset = (buildsetId, error) => ({
  type: BUILDSET_FETCH_FAIL,
  buildsetId,
  error
})

export function fetchBuildset(tenant, buildsetId) {
  return async function(dispatch) {
    dispatch(requestBuildset())
    try {
      const response = await API.fetchBuildset(tenant.apiPrefix, buildsetId)
      dispatch(receiveBuildset(buildsetId, response.data))
    } catch (error) {
      dispatch(failedBuildset(buildsetId, error))
    }
  }
}

const shouldFetchBuildset = (buildsetId, state) => {
  const buildset = state.build.buildsets[buildsetId]
  if (!buildset) {
    return true
  }
  if (buildset.isFetching) {
    return false
  }
  return false
}

export const fetchBuildsetIfNeeded = (tenant, buildsetId, force) => (
  dispatch, getState) => {
  if (force || shouldFetchBuildset(buildsetId, getState())) {
    return dispatch(fetchBuildset(tenant, buildsetId))
  }
}
