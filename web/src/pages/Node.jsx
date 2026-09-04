// Copyright 2026 Acme Gating, LLC
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

import React, { useEffect } from 'react'
import { useSelector, useDispatch } from 'react-redux'
import {
  useRouteMatch,
  Link,
} from 'react-router-dom'
import {
  Level,
  LevelItem,
  PageSection,
  PageSectionVariants,
  Title,
  Flex,
  FlexItem,
  List,
  ListItem,
} from '@patternfly/react-core'
import {
  LockIcon,
  NetworkIcon,
  OutlinedClockIcon,
  PortIcon,
  UserIcon,
  CloudTenantIcon,
  RegionsIcon,
  ZoneIcon,
  TachometerAltIcon,
  FingerprintIcon,
  TagIcon,
  InProgressIcon,
  OutlinedCommentsIcon,
  ReplicatorIcon,
  OutlinedHddIcon,
  RunningIcon,
  OutlinedCalendarIcon,
} from '@patternfly/react-icons'
import { fetchNodes, fetchNodesIfNeeded } from '../actions/nodes'
import { IconProperty } from '../MiscComponents'
import { JSONTree } from 'react-json-tree'
import moment_tz from 'moment-timezone'
import {
  getNodeStyle,
} from '../Misc'
import NodeTable from '../containers/nodes/NodeTable'
import { ReloadButton } from '../containers/Fetching'


const nodeAttrs = [
  {icon: <FingerprintIcon />, attr: 'id', title: 'ID'},
  {icon: <TagIcon />, attr: 'label', title: 'Label'},
  {icon: <RunningIcon />, attr: 'state', title: 'State'},
  {icon: <InProgressIcon />, attr: 'next_state', title: 'Next state'},
  {icon: <OutlinedCommentsIcon />, attr: 'comment', title: 'Comment'},
  {icon: <OutlinedClockIcon />, attr: 'hold_expiration', title: 'Hold expiration'},
  {icon: <OutlinedClockIcon />, attr: 'max_ready_age', title: 'Max ready age'},
  {icon: <OutlinedClockIcon />, attr: 'max_age', title: 'Max age'},
  {icon: <ReplicatorIcon />, attr: 'slot', title: 'Slot'},
  {icon: <OutlinedHddIcon />, attr: 'main_node_id', title: 'Main node'},
  {icon: <LockIcon />, attr: 'lock_holder', title: 'Locked by'},
  {icon: <FingerprintIcon />, attr: 'request_id', title: 'Request ID'},
  {icon: <OutlinedCalendarIcon />, attr: 'request_time', title: 'Request time'},
]

const providerAttrs = [
  {icon: <CloudTenantIcon />, attr: 'provider', title: 'Provider'},
  {icon: <CloudTenantIcon />, attr: 'cloud', title: 'Cloud'},
  {icon: <RegionsIcon />, attr: 'region', title: 'Region'},
  {icon: <ZoneIcon />, attr: 'az', title: 'Availability Zone'},
  {icon: <TachometerAltIcon />, attr: 'quota', title: 'Quota usage'},
]

const connectionAttrs = [
  {icon: <PortIcon />, attr: 'connection_type', title: 'Connection type'},
  {icon: <UserIcon />, attr: 'username', title: 'Username'},
  {icon: <NetworkIcon />, attr: 'interface_ip', title: 'Interface IP'},
  {icon: <NetworkIcon />, attr: 'public_ipv4', title: 'Public IPv4'},
  {icon: <NetworkIcon />, attr: 'private_ipv4', title: 'Private IPv4'},
  {icon: <NetworkIcon />, attr: 'public_ipv6', title: 'Public IPv6'},
  {icon: <NetworkIcon />, attr: 'private_ipv6', title: 'Private IPv6'},
]

function NodeInfoItem(tenant, darkMode, timezone, node, item) {
  let value = node[item.attr]
  let show = !!value  // Hide boring values
  if (item.attr == 'state') {
    const style = getNodeStyle(node)
    value = <span style={{color:style.color}}>{value}</span>
  } else if (item.attr == 'quota') {
    value = (
      <span style={{whiteSpace: 'pre-wrap'}}>
        <JSONTree
          data={value}
          hideRoot={true}
          sortObjectKeys={true}
          theme="default"
          invertTheme={!darkMode}
          shouldExpandNodeInitially={() => true}
        />
      </span>
    )
  } else if (item.attr == 'request_time') {
    value = moment_tz
      .utc(value)
      .tz(timezone)
      .format('YYYY-MM-DD HH:mm:ss')
  } else if (item.attr == 'slot') {
    show = (value !== null)  // Show slot 0
  } else if (item.attr == 'main_node_id') {
    value = (<Link to={`${tenant.linkPrefix}/node/${value}`}>{value}</Link>)
  }
  return (
    show ? (
      <IconProperty
        WrapElement={ListItem}
        icon={item.icon}
        key={item.attr}
        value={
          <>
            <strong>{item.title}</strong> {value}
          </>
        }
      />) : <></>)
}

function NodeInfo(tenant, darkMode, timezone, node) {
  return (
    <Flex className="zuul-node-attributes">
      <Flex flex={{ lg: 'flex_1' }}>
        <FlexItem>
          <List style={{ listStyle: 'none' }}>
            {nodeAttrs.map((item) =>
              {return NodeInfoItem(tenant, darkMode, timezone, node, item)}
            )}
          </List>
        </FlexItem>
      </Flex>
      <Flex flex={{ lg: 'flex_1' }}>
        <FlexItem>
          <List style={{ listStyle: 'none' }}>
            {providerAttrs.map((item) => {
              {return NodeInfoItem(tenant, darkMode, timezone, node, item)}
            })}
          </List>
        </FlexItem>
      </Flex>
      <Flex flex={{ lg: 'flex_1' }}>
        <FlexItem>
          <List style={{ listStyle: 'none' }}>
            {connectionAttrs.map((item) => {
              {return NodeInfoItem(tenant, darkMode, timezone, node, item)}
            })}
          </List>
        </FlexItem>
      </Flex>
    </Flex>
  )
}

function getRelatedNodes(nodes, node) {
  if (nodes && node) {
    const mainNodeId = node.main_node_id ? node.main_node_id : node.uuid
    return nodes.filter((e) => (
      e.main_node_id === mainNodeId ||
        e.uuid === mainNodeId))
  }
  return undefined
}

function NodePageComponent() {
  const match = useRouteMatch()
  const nodeId = match.params.nodeId

  const tenant = useSelector((state) => state.tenant)
  const nodes = useSelector((state) => state.nodes.nodes)
  const isFetching = useSelector((state) => state.status.isFetching)
  const darkMode = useSelector((state) => state.preferences.darkMode)
  const timezone = useSelector((state) => state.timezone)
  const dispatch = useDispatch()

  const node = nodes ? nodes.find((e) => e.id==nodeId) : undefined
  const relatedNodes = getRelatedNodes(nodes, node)

  useEffect(() => {
    document.title = `Zuul Node ${nodeId}`
    dispatch(fetchNodesIfNeeded(tenant))
  }, [dispatch, nodeId, tenant])

  return (
    <>
      <PageSection variant={darkMode ? PageSectionVariants.dark : PageSectionVariants.light}>
        <Level>
          <LevelItem>
          </LevelItem>
          <LevelItem>
            <ReloadButton
              isReloading={isFetching}
              reloadCallback={() => {dispatch(fetchNodes(tenant))}}
            />
          </LevelItem>
        </Level>
        <Title headingLevel="h2">Node {nodeId}</Title>

        {(node ? NodeInfo(tenant, darkMode, timezone, node) : <></>)}
      </PageSection>
      {(relatedNodes ?
        <PageSection variant={darkMode ? PageSectionVariants.dark : PageSectionVariants.light}>
        <Title headingLevel="h3">Related Nodes</Title>
          <NodeTable nodes={relatedNodes} fetching={false} />
        </PageSection>
        : <></>)}
    </>
  )
}

export default NodePageComponent
