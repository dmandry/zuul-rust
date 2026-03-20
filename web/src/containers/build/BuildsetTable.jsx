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
  Dropdown,
  DropdownItem,
  DropdownPosition,
  DropdownToggle,
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
  CogIcon,
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

function getRef(buildset) {
  return 'refs' in buildset ? buildset.refs[0] : buildset
}

function BuildsetTable({
  buildsets,
  fetching,
  onClearFilters,
  tenant,
  timezone,
  history,
}) {
  const [isDurationOpen, setIsDurationOpen] = React.useState(false)
  const [currentDuration, setCurrentDuration] = React.useState(
    'Buildset Duration'
  )

  const handleDurationSelect = (event) => {
    setIsDurationOpen(!isDurationOpen)
    setCurrentDuration(event.target.innerText)
  }

  const columns = [
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
      cellTransforms: [breakWord],
    },
    {
      title: <IconProperty icon={<CodeIcon />} value="Change" />,
      dataLabel: 'Change',
      transforms: [cellWidth(10)],
      cellTransforms: [truncate],
    },
    {
      title: <Dropdown
               isText
               isPlain
               position={DropdownPosition.left}
               onSelect={handleDurationSelect}
               toggle={
                 <DropdownToggle
                   onToggle={next => setIsDurationOpen(next)}
                   // Use a gear instead of a caret which looks likea sort indicator.
                   toggleIndicator={CogIcon}
                   style={{
                     padding: 0,
                     fontWeight: 'var(--pf-c-table--cell-FontWeight)',
                     fontSize: 'var(--pf-c-table--cell-FontSize)',
                     color: 'var(--pf-c-table--cell-Color)'
                   }}
                   id="toggle-id-duration"
                 >
                   <OutlinedClockIcon /> {currentDuration}
                 </DropdownToggle>
               }
               isOpen={isDurationOpen}
               dropdownItems={[
                 <DropdownItem key="buildset">Buildset Duration</DropdownItem>,
                 <DropdownItem key="overall">Overall Duration</DropdownItem>,
               ]}
               style={{ width: '100%', padding: 0 }}
             />,
      dataLabel: 'Duration',
      props: {style: {overflowY: 'visible'}},
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

  function createBuildsetRow(buildset) {
    const ref = getRef(buildset)
    const changeOrRefLink = buildExternalTableLink(ref)

    let duration
    if (currentDuration === 'Buildset Duration') {
      duration = moment.utc(buildset.last_build_end_time) -
            moment.utc(buildset.first_build_start_time)
    } else {
      duration = moment.utc(buildset.last_build_end_time) -
            moment.utc(buildset.event_timestamp)
    }

    return {
      // Pass the buildset's uuid as row id, so we can use it later on in the
      // action handler to build the link to the build result page for each row.
      id: buildset.uuid,
      cells: [
        {
          // To allow passing anything else than simple string values to a table
          // cell, we must use the title attribute.
          title: (
            <BuildResultWithIcon
              result={buildset.result}
              link={`${tenant.linkPrefix}/buildset/${buildset.uuid}`}>
              {ref.project}
            </BuildResultWithIcon>
          ),
        },
        {
          title: ref.branch ? ref.branch : ref.ref,
        },
        {
          title: buildset.pipeline,
        },
        {
          title: changeOrRefLink && changeOrRefLink,
        },
        {
          title: moment
            .duration(duration, 'ms')
            .format('h [hr] m [min] s [sec]'),
        },
        {
          title: moment_tz
            .utc(buildset.first_build_start_time)
            .tz(timezone)
            .format('YYYY-MM-DD HH:mm:ss'),
        },
        {
          title: moment_tz
            .utc(buildset.last_build_end_time)
            .tz(timezone)
            .format('YYYY-MM-DD HH:mm:ss'),
        },
        {
          title: (
            <BuildResult
              result={buildset.result}
              link={`${tenant.linkPrefix}/buildset/${buildset.uuid}`}
            >
            </BuildResult>
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
    // fetching row to be prepended by a "Project" column header. The other
    // column headers are not relevant here since we only have a single cell in
    // the fetching row.
    columns[0].dataLabel = ''
  } else {
    rows = buildsets.map((buildset) => createBuildsetRow(buildset))
    // This list of actions will be applied to each row in the table. For
    // row-specific actions we must evaluate the individual row data provided to
    // the onClick handler.
    actions = [
      {
        title: 'Show buildset result',
        onClick: (event, rowId, rowData) =>
          // The row's id contains the buildset's uuid, so we can use it to
          // build the correct link.
          history.push(`${tenant.linkPrefix}/buildset/${rowData.id}`),
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

      {/* Show an empty state in case we don't have any buildsets but are also
          not fetching */}
      {!fetching && buildsets.length === 0 && (
        <EmptyState>
          <EmptyStateIcon icon={BuildIcon} />
          <Title headingLevel="h1">No buildsets found</Title>
          <EmptyStateBody>
            No buildsets match this filter criteria. Remove some filters or
            clear all to show results.
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

BuildsetTable.propTypes = {
  buildsets: PropTypes.array.isRequired,
  fetching: PropTypes.bool.isRequired,
  onClearFilters: PropTypes.func.isRequired,
  tenant: PropTypes.object.isRequired,
  timezone: PropTypes.string.isRequired,
  history: PropTypes.object.isRequired,
}

export default connect((state) => ({
  tenant: state.tenant,
  timezone: state.timezone,
}))(BuildsetTable)
