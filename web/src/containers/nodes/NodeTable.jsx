// Copyright 2018, 2020 Red Hat, Inc
// Copyright 2022, 2024, 2026 Acme Gating, LLC
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

import React, { useState } from 'react'
import { useSelector, useDispatch } from 'react-redux'
import PropTypes from 'prop-types'
import {
  Table,
  TableVariant,
  TableHeader,
  TableBody,
  ActionsColumn,
  expandable,
} from '@patternfly/react-table'
import moment from 'moment'
import moment_tz from 'moment-timezone'
import {
  Spinner,
  Tooltip,
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
  IconProperty,
} from '../../MiscComponents'
import {
  formatProviderName,
  getNodeStyle,
  isAuthorized,
} from '../../Misc'
import { setNodeState } from '../../api'
import { addNotification } from '../../actions/notifications'
import { addApiError } from '../../actions/adminActions'
import { fetchNodesIfNeeded } from '../../actions/nodes'

function NodeTable(props) {
  const { nodes, fetching } = props
  const [collapsedRows, setCollapsedRows] = useState([])
  const setRowCollapsed = (idx, isCollapsing = true) =>
        setCollapsedRows(prevCollapsed => {
          const otherCollapsedRows = prevCollapsed.filter(r => r !== idx)
          return isCollapsing ?
                [...otherCollapsedRows, idx] : otherCollapsedRows
        })
  const isRowCollapsed = idx => collapsedRows.includes(idx)
  const tenant = useSelector((state) => state.tenant)
  const user = useSelector((state) => state.user)
  const timezone = useSelector((state) => state.timezone)
  const dispatch = useDispatch()

  const columns = [
    {
      title: (
        <IconProperty icon={<BuildIcon />} value="ID" />
      ),
      dataLabel: 'id',
      cellFormatters: [expandable],
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

  function renderNodeState(node) {
    const style = getNodeStyle(node)

    // 2192 is right arrow
    return (
      <>
        <span style={{color:style.color}}>{node.state}</span>
        {node.next_state &&
         (<>
            {'\u2192'} <span style={{color:style.color}}>{node.next_state}</span>
          </>)
        }
      </>
    )
  }

  function handleStateChange(nodeId, state) {
    setNodeState(tenant.apiPrefix, nodeId, state)
      .then(() => {
        dispatch(addNotification(
          {
            text: 'Node state updated.',
            type: 'success',
            status: '',
            url: '',
          }))
        dispatch(fetchNodesIfNeeded(tenant, true))
      })
      .catch(error => {
        dispatch(addApiError(error))
      })
  }

  function createNodeRow(rows, node, lastMainNode) {
    const state_time = typeof(node.state_time) === 'string' ?
          moment_tz.utc(node.state_time) :
          moment.unix(node.state_time)
    const r = [
      {title: node.id, props: {column: 'ID'}},
      {title: node.type.join(','), props: {column: 'Label' }},
      {title: renderNodeState(node), props: {column: 'State'}},
      {
        title: <Tooltip
                 content={
                   moment_tz
                     .utc(state_time)
                     .tz(timezone)
                     .format('YYYY-MM-DD HH:mm:ss')}>
                 <span>{state_time.fromNow()}</span>
               </Tooltip>,
        props: {column: 'Age'}
      },
      {title: node.lock_holder, props: {column: 'Locked'}},
      {title: formatProviderName(node.provider), props: {column: 'Provider'}},
      {title: node.comment, props: {column: 'Comment'}},
    ]
    if (node.uuid && isAuthorized(user, 'modify-node')) {
      r.push({title:
              <ActionsColumn items={[
                {
                  title: 'Set to HOLD',
                  onClick: () => handleStateChange(node.uuid, 'hold')
                },
                {
                  title: 'Set to OUTDATED',
                  onClick: () => handleStateChange(node.uuid, 'outdated')
                },
                {
                  title: 'Set to USED',
                  onClick: () => handleStateChange(node.uuid, 'used')
                },
              ]}/>
             })
    }
    const row = {
      id: rows.length,
      cells: r,
    }
    if (node.main_node_id) {
      row.parent = lastMainNode
    } else if (node._subnodes > 0) {
      row.isOpen = !isRowCollapsed(rows.length)
    }
    return row
  }

  function createFetchingRow() {
    const rows = [
      {
        heightAuto: true,
        cells: [
          {
            props: { colSpan: 8 },
            title: (
              <center>
                <Spinner size="xl" />
              </center>
            ),
          },
        ],
      },
    ]
    return rows
  }

  const haveNodes = nodes && nodes.length > 0

  let rows = []
  if (fetching) {
    rows = createFetchingRow()
    columns[0].dataLabel = ''
  } else {
    if (haveNodes) {
      rows = []
      let lastMainNode = null
      // We rely on the preprocessing that we perform in receiveNodes
      // to provide an array where each slot host is strictly followed
      // by its subnodes
      nodes.forEach(node => {
        rows.push(createNodeRow(rows, node, lastMainNode))
        if (!node.main_node_id) {
          lastMainNode = rows.length - 1
        }
      })
    }
  }

  return (
    <>
      <Table
        aria-label="Nodes Table"
        variant={TableVariant.compact}
        className="zuul-node-table"
        cells={columns}
        rows={rows}
        onCollapse={(_event, rowIndex, isOpen) => {
          setRowCollapsed(rowIndex, !isOpen)
        }}
      >
        <TableHeader />
        <TableBody />
        </Table>
    </>
  )
}


NodeTable.propTypes = {
  nodes: PropTypes.array,
  fetching: PropTypes.bool.isRequired,
}

export default NodeTable
