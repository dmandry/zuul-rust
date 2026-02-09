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
import moment from 'moment'
import moment_tz from 'moment-timezone'
import {
  PageSection,
  PageSectionVariants,
} from '@patternfly/react-core'
import {
  BuildIcon,
  LockIcon,
  OutlinedCalendarAltIcon,
  PencilAltIcon,
  RunningIcon,
  TagIcon,
  ZoneIcon,
} from '@patternfly/react-icons'
import {
  formatProviderName,
  getNodeStyle,
  isAuthorized,
} from '../Misc'
import {
  IconProperty,
} from '../MiscComponents'

import { setNodeState } from '../api'
import { addNotification } from '../actions/notifications'
import { addApiError } from '../actions/adminActions'
import { fetchNodesIfNeeded } from '../actions/nodes'
import { Fetchable } from '../containers/Fetching'

class NodesPage extends React.Component {
  static propTypes = {
    tenant: PropTypes.object,
    user: PropTypes.object,
    remoteData: PropTypes.object,
    dispatch: PropTypes.func
  }

  updateData = (force) => {
    this.props.dispatch(fetchNodesIfNeeded(this.props.tenant, force))
  }

  componentDidMount () {
    document.title = 'Zuul Nodes'
    if (this.props.tenant.name) {
      this.updateData()
    }
  }

  componentDidUpdate (prevProps) {
    if (this.props.tenant.name !== prevProps.tenant.name) {
      this.updateData()
    }
  }

  handleStateChange(nodeId, state) {
    setNodeState(this.props.tenant.apiPrefix, nodeId, state)
      .then(() => {
        this.props.dispatch(addNotification(
          {
            text: 'Node state updated.',
            type: 'success',
            status: '',
            url: '',
          }))
        this.props.dispatch(fetchNodesIfNeeded(this.props.tenant, true))
      })
      .catch(error => {
        this.props.dispatch(addApiError(error))
      })
  }

  renderNodeState(node) {
    const style = getNodeStyle(node)

    return <span style={{color:style.color}}>{node.state}</span>
  }

  render () {
    const { remoteData } = this.props
    const nodes = remoteData.nodes

    const columns = [
      {
        title: (
          <IconProperty icon={<BuildIcon />} value="ID" />
        ),
        dataLabel: 'id',
      },
      {
        title: (
          <IconProperty icon={<TagIcon />} value="Label" />
        ),
        dataLabel: 'label',
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
          <IconProperty icon={<LockIcon />} value="Locked" />
        ),
        dataLabel: 'locked',
      },
      {
        title: (
          <IconProperty icon={<ZoneIcon />} value="Provider" />
        ),
        dataLabel: 'provider',
      },
      {
        title: (
          <IconProperty icon={<PencilAltIcon />} value="Comment" />
        ),
        dataLabel: 'comment',
      },
      {
        title: '',
        dataLabel: 'action',
      },
    ]
    let rows = []
    nodes.forEach((node) => {
        const state_time = typeof(node.state_time) === 'string' ?
              moment_tz.utc(node.state_time) :
              moment.unix(node.state_time)
        const r = [
          {title: node.id, props: {column: 'ID'}},
          {title: node.type.join(','), props: {column: 'Label' }},
          {title: this.renderNodeState(node), props: {column: 'State'}},
          {title: state_time.fromNow(), props: {column: 'Age'}},
          {title: node.lock_holder, props: {column: 'Locked'}},
          {title: formatProviderName(node.provider), props: {column: 'Provider'}},
          {title: node.comment, props: {column: 'Comment'}},
        ]
      if (node.uuid && isAuthorized(this.props.user, 'modify-node')) {
        r.push({title:
                <ActionsColumn items={[
                  {
                    title: 'Set to HOLD',
                    onClick: () => this.handleStateChange(node.uuid, 'hold')
                  },
                  {
                    title: 'Set to USED',
                    onClick: () => this.handleStateChange(node.uuid, 'used')
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
            isFetching={remoteData.isFetching}
            fetchCallback={this.updateData}
          />
        </PageSection>

        <Table
          aria-label="Nodes Table"
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
  remoteData: state.nodes,
  user: state.user,
}))(NodesPage)
