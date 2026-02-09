// Copyright 2020 Red Hat, Inc
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

import React from 'react'
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import {
  EmptyState,
  EmptyStateBody,
  EmptyStateIcon,
  Spinner,
  Title,
} from '@patternfly/react-core'
import {
  OutlinedQuestionCircleIcon,
  HashtagIcon,
  BuildIcon,
  CodeBranchIcon,
  CubeIcon,
  OutlinedClockIcon,
  LockIcon,
  TrashIcon,
  FingerprintIcon,
} from '@patternfly/react-icons'
import {
  Table,
  TableHeader,
  TableBody,
  TableVariant,
} from '@patternfly/react-table'
import { Link } from 'react-router-dom'
import moment from 'moment'

import { autohold_delete } from '../../api'
import { addNotification } from '../../actions/notifications'
import { addAutoholdError } from '../../actions/adminActions'
import { fetchAutoholds } from '../../actions/autoholds'
import { IconProperty } from '../../MiscComponents'
import { isAuthorized } from '../../Misc'

function AutoholdTable(props) {
  const { autoholds, fetching, tenant, user, dispatch } = props
  const columns = [
    {
      title: <IconProperty icon={<FingerprintIcon />} value="ID" />,
      dataLabel: 'Request ID',
    },
    {
      title: <IconProperty icon={<CubeIcon />} value="Project" />,
      dataLabel: 'Project',
    },
    {
      title: <IconProperty icon={<BuildIcon />} value="Job" />,
      dataLabel: 'Job',
    },
    {
      title: <IconProperty icon={<CodeBranchIcon />} value="Ref Filter" />,
      dataLabel: 'Ref Filter',
    },
    {
      title: <IconProperty icon={<HashtagIcon />} value="Triggers" />,
      dataLabel: 'Triggers',
    },
    {
      title: <IconProperty icon={<OutlinedQuestionCircleIcon />} value="Reason" />,
      dataLabel: 'Reason',
    },
    {
      title: <IconProperty icon={<OutlinedClockIcon />} value="Hold for" />,
      dataLabel: 'Hold for',
    },
    {
      title: '',
      dataLabel: 'Delete',
    }
  ]

  function handleAutoholdDelete(requestId) {
    autohold_delete(tenant.apiPrefix, requestId)
      .then(() => {
        dispatch(addNotification(
          {
            text: 'Autohold request deleted successfully.',
            type: 'success',
            status: '',
            url: '',
          }))
        dispatch(fetchAutoholds(tenant))
      })
      .catch(error => {
        dispatch(addAutoholdError(error))
      })
  }

  function renderAutoholdDeleteButton(requestId) {
    return (
      <TrashIcon
        title="Delete Autohold request"
        style={{
          cursor: 'pointer',
          color: 'var(--pf-global--danger-color--100)',
        }}
        onClick={(event) => {
          event.preventDefault()
          handleAutoholdDelete(requestId)
        }} />
    )
  }

  function createAutoholdRow(autohold) {
    const count = autohold.current_count + '/' + autohold.max_count
    const node_expiration = (autohold.node_expiration === 0) ? 'Indefinitely' : moment.duration(autohold.node_expiration, 'seconds').humanize()
    const delete_button = isAuthorized(user, 'autohold') ? renderAutoholdDeleteButton(autohold.id) : ''

    return {
      cells: [
        {
          title: (
            <Link to={`${tenant.linkPrefix}/autohold/${autohold.id}`}>{autohold.id}</Link>
          ),
        },
        {
          title: autohold.project,
        },
        {
          title: autohold.job,
        },
        {
          title: autohold.ref_filter,
        },
        {
          title: count
        },
        {
          title: autohold.reason,
        },
        {
          title: node_expiration,
        },
        {
          title: delete_button
        },
      ]
    }
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

  let rows = []
  if (fetching) {
    rows = createFetchingRow()
    columns[0].dataLabel = ''
  } else {
    rows = autoholds.map((autohold) => createAutoholdRow(autohold))
  }

  return (
    <>
      <Table
        aria-label="Autohold Requests Table"
        variant={TableVariant.compact}
        cells={columns}
        rows={rows}
        className="zuul-table"
      >
        <TableHeader />
        <TableBody />
      </Table>

      {/* Show an empty state in case we don't have any autoholds but are also not
          fetching */}
      {!fetching && autoholds.length === 0 && (
        <EmptyState>
          <EmptyStateIcon icon={LockIcon} />
          <Title headingLevel="h1">No autohold requests found</Title>
          <EmptyStateBody>
            Nothing to display.
          </EmptyStateBody>
        </EmptyState>
      )}
    </>
  )
}

AutoholdTable.propTypes = {
  autoholds: PropTypes.array.isRequired,
  fetching: PropTypes.bool.isRequired,
  tenant: PropTypes.object,
  user: PropTypes.object,
  dispatch: PropTypes.func,
}

export default connect((state) => ({
  tenant: state.tenant,
  user: state.user,
}))(AutoholdTable)
