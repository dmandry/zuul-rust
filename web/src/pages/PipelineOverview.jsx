// Copyright 2024 BMW Group
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

import React, { useCallback, useEffect, useState, useMemo } from 'react'

import { useSelector, useDispatch } from 'react-redux'
import { withRouter, useLocation, useHistory } from 'react-router-dom'
import PropTypes from 'prop-types'
import * as moment_tz from 'moment-timezone'

import {
  Banner,
  Button,
  Gallery,
  GalleryItem,
  Level,
  LevelItem,
  PageSection,
  PageSectionVariants,
  Switch,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
  Tooltip,
  Flex,
  FlexItem,
} from '@patternfly/react-core'
import {
  StreamIcon,
  ExclamationTriangleIcon,
} from '@patternfly/react-icons'

import PipelineSummary from '../containers/status/PipelineSummary'
import PauseModal from '../containers/status/PauseModal'

import { fetchStatusIfNeeded } from '../actions/status'
import { clearQueue } from '../actions/statusExpansion'
import { Fetching, ReloadButton } from '../containers/Fetching'
import {
  getFiltersFromUrl,
  isFilterActive,
} from '../containers/FilterToolbar'
import {
  FilterToolbar,
  ToolbarStatsGroup,
  ToolbarStatsItem,
} from '../containers/FilterToolbarComponents'
import { getSearchKeyFromUrl, writeSearchKeyToUrl } from '../containers/SortDropdown'
import { SortDropdown } from '../containers/SortDropdownComponents'
import {
  clearFilters,
  filterInputValidation,
  filterPipelines,
  handleFilterChange,
} from '../containers/status/Filters'
import { EmptyBox } from '../containers/Errors'
import { countPipelineItems } from '../containers/status/Misc'
import { useDocumentVisibility, useInterval } from '../Hooks'
import { isAuthorized } from '../Misc'

function PipelineGallery({ pipelines, tenant, showAllPipelines, expandAll, isLoading, filters, onClearFilters, sortKey }) {
  // Filter out empty pipelines if necessary
  if (!showAllPipelines) {
    pipelines = pipelines.filter(ppl => ppl._count > 0)
  }

  switch(sortKey) {
    case 'name':
      pipelines = [...pipelines].sort((p1, p2) => p1.name.localeCompare(p2.name))
      break
    case 'length':
      pipelines = [...pipelines].sort((p1, p2) => p2._count - p1._count)
      break
    default:
      pipelines = [...pipelines]
      break
  }

  return (
    <>
      <Gallery
        hasGutter
        minWidths={{
          sm: '400px',
        }}
      >
        {pipelines.map(pipeline => (
          <GalleryItem key={pipeline.name}>
            <PipelineSummary pipeline={pipeline} tenant={tenant} showAllQueues={showAllPipelines} areAllJobsExpanded={expandAll} filters={filters} />
          </GalleryItem>
        ))}
      </Gallery>

      {!isLoading && pipelines.length === 0 && (
        <EmptyBox title="No items found"
          icon={StreamIcon}
          action="Clear all filters"
          onAction={onClearFilters}>
          No items match this filter criteria. Remove some filters or
          clear all to show results.
        </EmptyBox>
      )}
    </>
  )
}

PipelineGallery.propTypes = {
  pipelines: PropTypes.array,
  tenant: PropTypes.object,
  showAllPipelines: PropTypes.bool,
  expandAll: PropTypes.bool,
  isLoading: PropTypes.bool,
  filters: PropTypes.object,
  onClearFilters: PropTypes.func,
  sortKey: PropTypes.string,
}


function renderBanner(status)
{
  if (!status || !status.state) {
    return <></>
  }
  // This method is able to describe states that the client libraries
  // do not support creating (for example, buth discarding and pausing
  // the trigger event queue).
  const msgs = []
  const queues = []
  if (status.state.trigger_queue_discarding)
  {
    msgs.push('Discarding trigger events')
  }
  if (status.state.trigger_queue_paused) {
    queues.push('Trigger')
  }
  if (status.state.result_queue_paused) {
    queues.push('Result')
  }
  if (queues.length) {
    msgs.push(`${queues.join(', ')} event queue${queues.length>1?'s':''} paused`)
  }
  if (!msgs.length) {
    return <></>
  }
  const msg = `${msgs.join(', ')}: ${status.state.reason || 'no reason supplied'}`

  return (
    <Banner screenReaderText="Warning banner" variant="warning">
      <Flex spaceItems={{ default: 'spaceItemsSm' }}>
        <FlexItem>
          <ExclamationTriangleIcon />
        </FlexItem>
        <FlexItem>{msg}</FlexItem>
      </Flex>
    </Banner>
  )
}

function getPipelines(status, location, filterCategories) {
  let pipelines = []
  let stats = {}
  if (status) {
    const filters = getFiltersFromUrl(location, filterCategories)
    // we need to work on a copy of the state..pipelines, because when mutating
    // the original, we couldn't reset or change the filters without reloading
    // from the backend first.
    pipelines = structuredClone(status.pipelines)
    pipelines = filterPipelines(pipelines, filters, filterCategories, true)

    pipelines = pipelines.map(ppl => (
      countPipelineItems(ppl)
    ))
    stats = {
      trigger_event_queue: status.trigger_event_queue,
      management_event_queue: status.management_event_queue,
      last_reconfigured: status.last_reconfigured,
    }
  }
  return {
    pipelines,
    stats,
  }
}

function PipelineOverviewPageComponent() {
  const status = useSelector((state) => state.status.status)
  const user = useSelector((state) => state.user)

  const filterCategories = useMemo(() =>
    [
      {
        key: 'change',
        title: 'Change',
        placeholder: 'Filter by Change...',
        type: 'search',
        fuzzy: true,
      },
      {
        key: 'project',
        title: 'Project',
        placeholder: 'Filter by Project...',
        type: 'search',
        fuzzy: true,
      },
      {
        key: 'queue',
        title: 'Queue',
        placeholder: 'Filter by Queue...',
        type: 'select',
        // the last filter part makes sure we only provide options for queues
        // which have a non-empty name
        options: status ? status.pipelines.map(p => p.change_queues).flat().map(q => q.name).filter(n => n) : [],
        fuzzy: true,
      },
      {
        key: 'pipeline',
        title: 'Pipeline',
        placeholder: 'Filter by Pipeline...',
        type: 'select',
        options: status ? status.pipelines.map(p => p.name) : [],
        fuzzy: true,
      },
    ],
    [status])

  const location = useLocation()
  const history = useHistory()
  const filters = getFiltersFromUrl(location, filterCategories)
  const filterActive = isFilterActive(filters)

  const [showPauseModal, setShowPauseModal] = useState(false)
  const [showAllPipelines, setShowAllPipelines] = useState(
    filterActive || localStorage.getItem('zuul_show_all_pipelines') === 'true')
  const [expandAll, setExpandAll] = useState(
    localStorage.getItem('zuul_overview_expand_all') === 'true')
  const [isReloading, setIsReloading] = useState(false)

  const isDocumentVisible = useDocumentVisibility()

  const { pipelines, stats } = useMemo(() => getPipelines(status, location, filterCategories), [status, location, filterCategories])

  const isFetching = useSelector((state) => state.status.isFetching)
  const tenant = useSelector((state) => state.tenant)
  const darkMode = useSelector((state) => state.preferences.darkMode)
  const autoReload = useSelector((state) => state.preferences.autoReload)
  const timezone = useSelector((state) => state.timezone)
  const dispatch = useDispatch()

  const sortKeys = [
      {key: 'none', title: 'Preset'},
      {key: 'name', title: 'Name'},
      {key: 'length', title: 'Length'},
  ]
  const [currentSortKey, setCurrentSortKey] = useState(getSearchKeyFromUrl(location, sortKeys) || sortKeys[0])

  const onSortKeyChanged = (sortKey) => {
    setCurrentSortKey(sortKey)
    writeSearchKeyToUrl(sortKey, location, history)
  }

  const onShowAllPipelinesToggle = (isChecked) => {
    setShowAllPipelines(isChecked)
    localStorage.setItem('zuul_show_all_pipelines', isChecked.toString())
  }

  const onExpandAllToggle = (isChecked) => {
    setExpandAll(isChecked)
    localStorage.setItem('zuul_overview_expand_all', isChecked.toString())
    dispatch(clearQueue())
  }

  const onFilterChanged = (newFilters) => {
    handleFilterChange(newFilters, filterCategories, location, history)
    // show all pipelines when filtering, hide when not
    setShowAllPipelines(
      isFilterActive(newFilters) || localStorage.getItem('zuul_show_all_pipelines') === 'true')
  }

  const onClearFilters = () => {
    clearFilters(location, history, filterCategories)
    // reset `showAllPipelines` when clearing filters
    setShowAllPipelines(localStorage.getItem('zuul_show_all_pipelines') === 'true')
  }

  const updateData = useCallback((tenant) => {
    if (tenant.name) {
      setIsReloading(true)
      dispatch(fetchStatusIfNeeded(tenant))
        .then(() => {
          setIsReloading(false)
        })
    }
  }, [setIsReloading, dispatch])

  useEffect(() => {
    document.title = 'Zuul Status'
    // Initial data fetch
    if (tenant.name) {
      dispatch(fetchStatusIfNeeded(tenant))
    }
  }, [tenant, dispatch])

  // Subsequent data fetches every 5 seconds if auto-reload is enabled
  useInterval(() => {
    if (isDocumentVisible && autoReload) {
      updateData(tenant)
    }
    // Reset the interval on a manual refresh
  }, isReloading ? null : 5000)

  // Only show the fetching component on the initial data fetch, but
  // not on subsequent reloads, as this would overlay the page data.
  if (!isReloading && isFetching) {
    return <Fetching />
  }

  const allPipelinesSwitch = (
    <Switch
      className="zuul-show-all-switch"
      id="all-pipeline-switch"
      aria-label="Show all pipelines"
      label="Show all pipelines"
      isReversed
      isChecked={showAllPipelines}
      isDisabled={filterActive}
      onChange={onShowAllPipelinesToggle}
    />
  )

  let trigger_events = stats.trigger_event_queue ? stats.trigger_event_queue.length : '0'
  let management_events = stats.management_event_queue ? stats.management_event_queue.length : '0'

  return (
    <>

      {renderBanner(status)}

      <PageSection
        variant={darkMode ? PageSectionVariants.dark : PageSectionVariants.light}
        className="zuul-toolbar-section"
      >
        <Level>
          <LevelItem>
            <FilterToolbar
              filterCategories={filterCategories}
              onFilterChange={onFilterChanged}
              filters={filters}
              filterInputValidation={filterInputValidation}
            >
              <ToolbarItem>
                <SortDropdown sortKeys={sortKeys} selectedSortKey={currentSortKey} onSortKeyChange={onSortKeyChanged}/>
              </ToolbarItem>
              <ToolbarItem>
                {filterActive ?
                  <Tooltip content="Disabled when filtering">{allPipelinesSwitch}</Tooltip> :
                  allPipelinesSwitch}
              </ToolbarItem>
              <ToolbarItem>
                <Switch
                  className="zuul-show-all-switch"
                  aria-label="Expand all"
                  label="Expand all"
                  isReversed
                  onChange={onExpandAllToggle}
                  isChecked={expandAll}
                />
              </ToolbarItem>
            </FilterToolbar>
          </LevelItem>
          <LevelItem>
            <Toolbar>
              <ToolbarContent style={{paddingRight: '0'}}>
                {isAuthorized(user, 'set-tenant-state') && (
                  <ToolbarItem>
                    <Button onClick={() => {setShowPauseModal(true)}}>
                      Manage Events
                    </Button>
                  </ToolbarItem>
                )}

                <ToolbarStatsGroup>
                  <ToolbarStatsItem
                    name="events"
                    value={`${trigger_events} / ${management_events}`}
                    tooltipContent={
                      <div>
                        Trigger events: {trigger_events} <br />
                        Management events: {management_events}
                      </div>}
                  />
                  <ToolbarStatsItem
                    name="reconfigured"
                    value={moment_tz.utc(stats.last_reconfigured).tz(timezone).fromNow()}
                    reverse
                    tooltipContent={
                      <div>
                        Last reconfigured: <br />
                        {moment_tz.utc(stats.last_reconfigured).tz(timezone).format('llll')}
                      </div>
                    }
                    link={`${tenant.linkPrefix}/system-events`}
                  />
                  <ToolbarItem>
                    <ReloadButton
                      isReloading={isReloading}
                      reloadCallback={() => updateData(tenant)}
                    />
                  </ToolbarItem>
                </ToolbarStatsGroup>
              </ToolbarContent>
            </Toolbar>
          </LevelItem>
        </Level>
      </PageSection >
      <PageSection
        variant={darkMode ? PageSectionVariants.dark : PageSectionVariants.light}
        className="zuul-main-section"
      >
        <PipelineGallery
          pipelines={pipelines}
          tenant={tenant}
          showAllPipelines={showAllPipelines}
          expandAll={expandAll}
          isLoading={isFetching}
          filters={filters}
          onClearFilters={onClearFilters}
          sortKey={currentSortKey.key}
        />
      </PageSection>
      <PauseModal
        isOpen={showPauseModal}
        setOpen={setShowPauseModal}
      />
    </>
  )
}

const PipelineOverviewPage = withRouter(PipelineOverviewPageComponent)
export default PipelineOverviewPage
