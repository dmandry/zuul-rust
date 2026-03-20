// Copyright 2020 BMW Group
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
  Button,
  EmptyState,
  EmptyStateBody,
  EmptyStateIcon,
  EmptyStateSecondaryActions,
  Spinner,
  Title,
} from '@patternfly/react-core'
import {
  BuildIcon,
  CodeBranchIcon,
  CodeIcon,
  CubeIcon,
  OutlinedCalendarAltIcon,
  OutlinedClockIcon,
  PollIcon,
  StreamIcon,
} from '@patternfly/react-icons'
import {
  Table,
  TableHeader,
  TableBody,
  TableVariant,
  truncate,
  breakWord,
  cellWidth,
} from '@patternfly/react-table'
import momentDurationSetup from 'moment-duration-format'
import moment from 'moment'
import moment_tz from 'moment-timezone'
momentDurationSetup(moment_tz)

import { BuildResult, BuildResultWithIcon } from './Misc'
import { buildExternalTableLink } from '../../Misc'
import { IconProperty } from '../../MiscComponents'

function getRef(build) {
  return 'project' in build ? build : build.ref
}

function BuildTable({
  builds,
  fetching,
  onClearFilters,
  tenant,
  timezone,
  history,
}) {
  const columns = [
    {
      title: <IconProperty icon={<BuildIcon />} value="Job" />,
      dataLabel: 'Job',
      cellTransforms: [breakWord],
    },
    {
      title: <IconProperty icon={<CubeIcon />} value="Project" />,
      dataLabel: 'Project',
      cellTransforms: [breakWord],
    },
    {
      title: <IconProperty icon={<CodeBranchIcon />} value="Branch" />,
      dataLabel: 'Branch',
      cellTransforms: [breakWord],
    },
    {
      title: <IconProperty icon={<StreamIcon />} value="Pipeline" />,
      dataLabel: 'Pipeline',
    },
    {
      title: <IconProperty icon={<CodeIcon />} value="Change" />,
      dataLabel: 'Change',
      transforms: [cellWidth(10)],
      cellTransforms: [truncate],
    },
    {
      title: <IconProperty icon={<OutlinedClockIcon />} value="Duration" />,
      dataLabel: 'Duration',
    },
    {
      title: (
        <IconProperty icon={<OutlinedCalendarAltIcon />} value="Start time" />
      ),
      dataLabel: 'Start time',
    },
    {
      title: (
        <IconProperty icon={<OutlinedCalendarAltIcon />} value="End time" />
      ),
      dataLabel: 'End time',
    },
    {
      title: <IconProperty icon={<PollIcon />} value="Result" />,
      dataLabel: 'Result',
    },
  ]

  function createBuildRow(build) {
    const ref = getRef(build)
    const changeOrRefLink = buildExternalTableLink(ref)

    return {
      // Pass the build's uuid as row id, so we can use it later on in the
      // action handler to build the link to the build result page for each row.
      id: build.uuid,
      cells: [
        {
          // To allow passing anything else than simple string values to a table
          // cell, we must use the title attribute.
          title: (
            <>
              <BuildResultWithIcon
                result={build.result}
                colored={build.voting}
                link={`${tenant.linkPrefix}/build/${build.uuid}`}
              >
                {build.job_name}
                {!build.voting && ' (non-voting)'}
              </BuildResultWithIcon>
            </>
          ),
        },
        {
          title: ref.project,
        },
        {
          title: ref.branch ? ref.branch : ref.ref,
        },
        {
          title: build.pipeline,
        },
        {
          title: changeOrRefLink && changeOrRefLink,
        },
        {
          title: moment
            .duration(build.duration, 'seconds')
            .format('h [hr] m [min] s [sec]'),
        },
        {
          title: moment_tz
            .utc(build.start_time)
            .tz(timezone)
            .format('YYYY-MM-DD HH:mm:ss'),
        },
        {
          title: moment_tz
            .utc(build.end_time)
            .tz(timezone)
            .format('YYYY-MM-DD HH:mm:ss'),
        },
        {
          title: (
            <BuildResult
              result={build.result}
              link={`${tenant.linkPrefix}/build/${build.uuid}`}
              colored={build.voting}
            />
          ),
        },
      ],
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
  // For the fetching row we don't need any actions, so we keep them empty by
  // default.
  let actions = []
  if (fetching) {
    rows = createFetchingRow()
    // The dataLabel property is used to show the column header in a list-like
    // format for smaller viewports. When we are fetching, we don't want the
    // fetching row to be prepended by a "Job" column header. The other column
    // headers are not relevant here since we only have a single cell in the
    // fetcihng row.
    columns[0].dataLabel = ''
  } else {
    rows = builds.map((build) => createBuildRow(build))
    // This list of actions will be applied to each row in the table. For
    // row-specific actions we must evaluate the individual row data provided to
    // the onClick handler.
    actions = [
      {
        title: 'Show build result',
        onClick: (event, rowId, rowData) =>
          // The row's id contains the build's uuid, so we can use this to build
          // the correct link.
          history.push(`${tenant.linkPrefix}/build/${rowData.id}`),
      },
    ]
  }

  return (
    <>
      <Table
        aria-label="Builds Table"
        variant={TableVariant.compact}
        cells={columns}
        rows={rows}
        actions={actions}
        className="zuul-table"
      >
        <TableHeader />
        <TableBody />
      </Table>

      {/* Show an empty state in case we don't have any builds but are also not
          fetching */}
      {!fetching && builds.length === 0 && (
        <EmptyState>
          <EmptyStateIcon icon={BuildIcon} />
          <Title headingLevel="h1">No builds found</Title>
          <EmptyStateBody>
            No builds match this filter criteria. Remove some filters or clear
            all to show results.
          </EmptyStateBody>
          <EmptyStateSecondaryActions>
            <Button variant="link" onClick={onClearFilters}>
              Clear all filters
            </Button>
          </EmptyStateSecondaryActions>
        </EmptyState>
      )}
    </>
  )
}

BuildTable.propTypes = {
  builds: PropTypes.array.isRequired,
  fetching: PropTypes.bool.isRequired,
  onClearFilters: PropTypes.func.isRequired,
  tenant: PropTypes.object.isRequired,
  timezone: PropTypes.string.isRequired,
  history: PropTypes.object.isRequired,
}

export default connect((state) => ({
  tenant: state.tenant,
  timezone: state.timezone,
}))(BuildTable)
