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
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import {
  Table,
  TableVariant,
  TableHeader,
  TableBody,
  ActionsColumn,
} from '@patternfly/react-table'
import * as moment_tz from 'moment-timezone'
import {
  PageSection,
  PageSectionVariants,
  Button,
} from '@patternfly/react-core'
import {
  ContainerNodeIcon,
  SquareIcon,
  BuildIcon,
  BundleIcon,
  FingerprintIcon,
  OutlinedCalendarAltIcon,
  RunningIcon,
  StreamIcon,
  TagIcon,
} from '@patternfly/react-icons'
import { Link } from 'react-router-dom'
import {
  getNodeStyle,
  isAuthorized,
} from '../Misc'
import {
  IconProperty,
} from '../MiscComponents'

import { deleteNodesetRequest } from '../api'
import { addNotification } from '../actions/notifications'
import { addApiError } from '../actions/adminActions'
import { fetchNodesIfNeeded } from '../actions/nodes'
import { fetchNodesetRequestsIfNeeded } from '../actions/nodesetRequests'
import { Fetchable } from '../containers/Fetching'
import NodePopover from '../containers/nodes/NodePopover'


function NodeSquare({ node }) {
  const nodeStyle = getNodeStyle(node)
  return (
    <Button
      variant="plain"
      className={`zuul-item-square zuul-item-square-${nodeStyle.variant}`}
    >
      <SquareIcon />
    </Button>
  )
}

NodeSquare.propTypes = {
  node: PropTypes.object,
}

function NodeSquareWithPopover({ node }) {
  return (
    <NodePopover
      node={node}
      triggerElement={<NodeSquare node={node} />}
    />
  )
}

NodeSquareWithPopover.propTypes = {
  node: PropTypes.object,
}

function NodeStates({ request, allNodes }) {
  if (!request.provider_node_data) {
    return <></>
  }

  const requestNodes = request.provider_node_data.map((nodeData) => (
    allNodes[nodeData.uuid])).filter(n => n !== undefined)

  return (
    <>
      {requestNodes.map((node) => (
        <NodeSquareWithPopover key={node.uuid} node={node}/>
      ))}
    </>
  )
}

NodeStates.propTypes = {
  request: PropTypes.object,
  allNodes: PropTypes.object,
}

class NodesetRequestsPage extends React.Component {
  static propTypes = {
    tenant: PropTypes.object,
    user: PropTypes.object,
    remoteRequests: PropTypes.object,
    remoteNodes: PropTypes.object,
    dispatch: PropTypes.func
  }

  updateData = (force) => {
    this.props.dispatch(fetchNodesetRequestsIfNeeded(this.props.tenant, force))
    this.props.dispatch(fetchNodesIfNeeded(this.props.tenant, force))
  }

  componentDidMount () {
    document.title = 'Zuul Nodeset Requests'
    if (this.props.tenant.name) {
      this.updateData()
    }
  }

  componentDidUpdate (prevProps) {
    if (this.props.tenant.name !== prevProps.tenant.name) {
      this.updateData()
    }
  }

  handleDelete(requestId) {
    deleteNodesetRequest(this.props.tenant.apiPrefix, requestId)
      .then(() => {
        this.props.dispatch(addNotification(
          {
            text: 'Nodeset request deleted.',
            type: 'success',
            status: '',
            url: '',
          }))
        this.props.dispatch(fetchNodesetRequestsIfNeeded(this.props.tenant, true))
      })
      .catch(error => {
        this.props.dispatch(addApiError(error))
      })
  }

  render () {
    const { remoteRequests, remoteNodes } = this.props
    const nodesetRequests = remoteRequests.requests
    const allNodes = remoteNodes.nodes.reduce((obj, item) =>
      ({...obj, [item.uuid]: item}), {})
    const columns = [
      {
        title: (
          <IconProperty icon={<FingerprintIcon />} value="UUID" />
        ),
        dataLabel: 'uuid',
      },
      {
        title: (
          <IconProperty icon={<TagIcon />} value="Labels" />
        ),
        dataLabel: 'labels',
      },
      {
        title: (
          <IconProperty icon={<ContainerNodeIcon />} value="Nodes" />
        ),
        dataLabel: 'nodes',
      },
      {
        title: (
          <IconProperty icon={<RunningIcon />} value="State" />
        ),
        dataLabel: 'state',
      },
      {
        title: (
          <IconProperty icon={<OutlinedCalendarAltIcon />} value="Age" />
        ),
        dataLabel: 'age',
      },
      {
        title: (
          <IconProperty icon={<BundleIcon />} value="Buildset" />
        ),
        dataLabel: 'buildset',
      },
      {
        title: (
          <IconProperty icon={<StreamIcon />} value="Pipeline" />
        ),
        dataLabel: 'pipeline',
      },
      {
        title: (
          <IconProperty icon={<BuildIcon />} value="Job" />
        ),
        dataLabel: 'provider',
      },
      {
        title: '',
        dataLabel: 'action',
      },
    ]
    let rows = []
    nodesetRequests.forEach((request) => {
      const r = [
        {title: request.uuid, props: {column: 'UUID'}},
        {title: request.labels.join(','), props: {column: 'Labels' }},
        {title: <NodeStates request={request} allNodes={allNodes} />, props: {column: 'Nodes'}},
        {title: request.state, props: {column: 'State'}},
        {title: moment_tz.utc(request.request_time).fromNow(), props: {column: 'Age'}},
        {title: <Link to={`${this.props.tenant.linkPrefix}/buildset/${request.buildset_uuid}`}>{request.buildset_uuid}</Link>, props: {column: 'Buildset'}},
        {title: request.pipeline_name, props: {column: 'Pipeline'}},
        {title: request.job_name, props: {column: 'Job'}},
      ]

      if (isAuthorized(this.props.user, 'modify-nodeset-request')) {
        r.push({title:
                <ActionsColumn items={[
                  {
                    title: 'Delete',
                    onClick: () => this.handleDelete(request.uuid)
                  },
                ]}/>
               })
      }
      rows.push({cells: r})
    })
    return (
      <PageSection variant={PageSectionVariants.light}>
        <PageSection style={{paddingRight: '5px'}}>
          <Fetchable
            isFetching={remoteRequests.isFetching}
            fetchCallback={this.updateData}
          />
        </PageSection>

        <Table
          aria-label="Nodeset Requests Table"
          variant={TableVariant.compact}
          cells={columns}
          rows={rows}
          className="zuul-table"
        >
          <TableHeader />
          <TableBody />
        </Table>
      </PageSection>
    )
  }
}

export default connect(state => ({
  tenant: state.tenant,
  remoteRequests: state.nodesetRequests,
  remoteNodes: state.nodes,
  user: state.user,
}))(NodesetRequestsPage)
