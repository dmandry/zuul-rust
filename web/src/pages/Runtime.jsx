// Copyright 2022 Acme Gating, LLC
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

import React, { useEffect, useState, useCallback, useMemo } from 'react'
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import { useHistory, useLocation } from 'react-router-dom'
import {
  Dropdown,
  DropdownItem,
  DropdownPosition,
  DropdownToggle,
  DatePicker,
  PageSection,
  PageSectionVariants,
  Text,
  TextContent,
  Toolbar,
  ToolbarContent,
  ToolbarGroup,
  ToolbarItem,
  ToolbarToggleGroup,
  HelperText,
  HelperTextItem,
} from '@patternfly/react-core'
import { fetchBuildTimes } from '../api'
import {
  Chart,
  ChartAxis,
  ChartScatter,
  ChartThemeColor,
  ChartThemeVariant,
  ChartVoronoiContainer,
  getCustomTheme,
} from '@patternfly/react-charts'
import momentDurationSetup from 'moment-duration-format'
import moment from 'moment'
import moment_tz from 'moment-timezone'
momentDurationSetup(moment_tz)

import FreezeJobToolbar from '../containers/freezejob/FreezeJobToolbar'

/* This should be able to be removed in PF5 due to built in theming /*
   Reference for keys:
   https://github.com/patternfly/patternfly-react/blob/62ec17cdac823d3f993c04bb3b22c2811dec4cb7/packages/react-charts/src/components/ChartTheme/themes/base-theme.ts
*/
const DarkTheme={
  axis: {
    style: {
      grid: {
        stroke: '#e0e0e0',
      },
      ticks: {
        stroke: '#e0e0e0',
      },
      axis: {
        stroke: '#e0e0e0',
      },
      axisLabel: {
        stroke: '#e0e0e0',
      },
      tickLabels: {
        stroke: '#e0e0e0',
      },
    },
  },
  legend: {
    style: {
      labels: {
        stroke: '#e0e0e0',
      },
    },
  },
  scatter: {
    style: {
      labels: {
        fill: '#e0e0e0',
        stroke: '#e0e0e0',
      },
    },
  },
}

const LightTheme={
  axis: {
    style: {
      grid: {
        stroke: '#ccc',
      },
      ticks: {
        stroke: '#ccc',
      },
    },
  },
}

function toTime(x, tz) {
  return moment_tz.utc(new Date(x)).tz(tz)
}

function TestChart(builds, preferences, timezone) {
  const myTheme = getCustomTheme(ChartThemeColor.blue, ChartThemeVariant.light,
                                 preferences.darkMode ? DarkTheme : LightTheme)
  let results = {SUCCESS: [], FAILURE: []}
  builds.forEach(build => {
    if (results[build.result] === undefined) {
      results[build.result] = []
    }
    results[build.result].push({
      name: toTime(build.end_time, timezone),
      x: toTime(build.end_time, timezone),
      y: build.duration,
      y_human: moment.duration(build.duration, 'seconds').format('h[h]m[m]s[s]'),
    })
  })
  const title = `Runtimes of ${builds[0].job_name} on ${builds[0].project}@${builds[0].branch}`

  return (
  <div>
    <TextContent>
      <Text component="h2" style={{paddingLeft: 50}}>
        {title}
      </Text>
    </TextContent>
    <Chart
      ariaTitle={title}
      containerComponent={<ChartVoronoiContainer labels={({ datum }) => `${datum.name}: ${datum.y_human}`} constrainToVisibleArea />}
      legendData={[{ name: 'Success' }, { name: 'Failure' }]}
      legendOrientation="vertical"
      legendPosition="right"
      height={500}
      name="chart1"
      theme={myTheme}
      padding={{
        bottom: 50,
        left: 100,  // Adjusted to accommodate axis
        right: 100, // Adjusted to accommodate legend
        top: 10
      }}
      width={1200}
      scale={{x: 'time', y: 'linear'}}
    >
      <ChartAxis
        tickValues={[toTime(builds[0].end_time, timezone),
                     toTime(builds.slice(-1)[0].end_time, timezone)]}
        tickFormat={(t) => t.format('YYYY-MM-DD')}
      />
      <ChartAxis
        dependentAxis
        tickFormat={(t) => moment.duration(t, 'seconds').format('h[h]m[m]s[s]')}
      />
      <ChartScatter
        data={results['SUCCESS']}
        style={{data: {fill: '#0066cc'}}}
      />
      <ChartScatter
        data={results['FAILURE']}
        style={{data: {fill: '#8bc1f7'}}}
      />
    </Chart>
  </div>
  )
}


function RuntimePage(props) {
  const { tenant, preferences, timezone } = props

  const [currentPipeline, setCurrentPipeline] = useState()
  const [currentProject, setCurrentProject] = useState()
  const [currentBranch, setCurrentBranch] = useState()
  const [currentJobName, setCurrentJobName] = useState()
  const [currentStartTime, setCurrentStartTime] = useState()
  const [currentEndTime, setCurrentEndTime] = useState()
  const [currentPreset, setCurrentPreset] = useState()
  const [isPresetOpen, setIsPresetOpen] = useState(false)
  const [buildList, setBuildList] = useState()
  const history = useHistory()
  const location = useLocation()
  const presets = useMemo( () => {
    return [
      [undefined, "None"],
      [24*60*60, "24 hours"],
      [7*24*60*60, "7 days"],
      [30*24*60*60, "30 days"],
      [90*24*60*60, "90 days"],
      [180*24*60*60, "6 months"],
      [365*24*60*60, "1 year"],
      [2*365*24*60*60, "2 years"],
    ]
  }, [])

  if (!currentBranch) {
    const urlParams = new URLSearchParams(location.search)
    const pipeline = urlParams.get('pipeline')
    const project = urlParams.get('project')
    const branch = urlParams.get('branch')
    const jobName = urlParams.get('job_name')
    const startTime = urlParams.get('start_time')
    const endTime = urlParams.get('end_time')
    const preset = urlParams.get('preset')
    if (pipeline && branch && project && jobName) {
      setCurrentPipeline(pipeline)
      setCurrentProject(project)
      setCurrentBranch(branch)
      setCurrentJobName(jobName)
    }
    if (startTime) {
      setCurrentStartTime(startTime)
    }
    if (endTime) {
      setCurrentEndTime(endTime)
    }
    if (preset) {
      setCurrentPreset(preset)
    }
  }

  const updateData = useCallback(() => {
    let queryString=`&pipeline=${currentPipeline}&project=${currentProject}&branch=${currentBranch}&job_name=${currentJobName}`
    if (currentStartTime || currentEndTime || currentPreset) {
      queryString += '&limit=5000'
    }
    if (currentPreset) {
      const seconds = presets.filter((x) => {
        return x[1] == currentPreset
      })[0][0]
      const end = new Date()
      const start = new Date(end - (seconds * 1000))
      console.log(currentPreset, seconds, start, end)
      queryString += `&start_time=${start.toISOString()}&end_time=${end.toISOString()}`
    }
    else {
      if (currentStartTime) {
        queryString += `&start_time=${currentStartTime}`
      }
      if (currentEndTime) {
        queryString += `&end_time=${currentEndTime}`
     }
    }
    fetchBuildTimes(tenant.apiPrefix, queryString).then((response) => {
      const { compare } = Intl.Collator('en-US')
      const builds = response.data
      builds.sort((a, b) => compare(a.end_time, b.end_time))
      setBuildList(builds)
    })
  }, [currentPipeline, currentProject,
      currentBranch, currentJobName, currentStartTime, currentEndTime,
      currentPreset, tenant.apiPrefix, presets])

  useEffect(() => {
    document.title = 'Zuul Job Runtimes'
    if (currentPipeline && currentProject && currentBranch && currentJobName) {
      updateData()
    }
  }, [updateData, tenant, currentPipeline, currentProject,
      currentBranch, currentJobName, currentStartTime, currentEndTime,
      currentPreset])

  function onPipelineToolbarChange(pipeline, project, branch, jobName) {
    setCurrentPipeline(pipeline)
    setCurrentProject(project)
    setCurrentBranch(branch)
    setCurrentJobName(jobName)

    onInputChange(pipeline, project, branch, jobName,
                  currentStartTime, currentEndTime, currentPreset)
  }

  function onStartTimeChange(startTime) {
    if (startTime) {
      startTime = startTime.toISOString()
    }
    setCurrentStartTime(startTime)
    onInputChange(currentPipeline, currentProject, currentBranch, currentJobName,
                  startTime, currentEndTime, currentPreset)
  }

  function onEndTimeChange(endTime) {
    if (endTime) {
      endTime = endTime.toISOString()
    }
    setCurrentEndTime(endTime)
    onInputChange(currentPipeline, currentProject, currentBranch, currentJobName,
                  currentStartTime, endTime, currentPreset)
  }

  function onPresetChange(event) {
    let preset = event.target.innerText
    preset = preset == "None" ? undefined : preset
    setCurrentPreset(preset)
    onInputChange(currentPipeline, currentProject, currentBranch, currentJobName,
                  currentStartTime, currentEndTime, preset)
    setIsPresetOpen(false)
  }

  function onInputChange(pipeline, project, branch, jobName, startTime, endTime, preset) {
    const searchParams = new URLSearchParams('')
    searchParams.append('pipeline', pipeline)
    searchParams.append('project', project)
    searchParams.append('branch', branch)
    searchParams.append('job_name', jobName)
    if (preset) {
      searchParams.append('preset', preset)
    } else {
      if (startTime) {
        searchParams.append('start_time', startTime)
      }
      if (endTime) {
        searchParams.append('end_time', endTime)
      }
    }
    history.push({
      pathname: location.pathname,
      search: searchParams.toString(),
    })
  }

  function renderBuilds(builds) {
    if (builds && builds.length > 0) {
      return (TestChart(builds, preferences, timezone))
    } else {
      return <></>
    }
  }

  const startDateValue = (currentStartTime && currentStartTime.length > 10 ) ?
        currentStartTime.substring(0, 10) : undefined
  const endDateValue = (currentEndTime && currentEndTime.length > 10 ) ?
        currentEndTime.substring(0, 10) : undefined

  return (
    <>
      <PageSection variant={PageSectionVariants.light}>
        <TextContent>
          <Text component="h1">Job Runtimes</Text>
        </TextContent>
        <FreezeJobToolbar
          onChange={onPipelineToolbarChange}
          defaultPipeline={currentPipeline}
          defaultProject={currentProject}
          defaultBranch={currentBranch}
          defaultJob={currentJobName}
          buttonText="Fetch"
        />
        <Toolbar collapseListedFiltersBreakpoint="md">
          <ToolbarContent>
            <ToolbarToggleGroup breakpoint="md">
              <ToolbarGroup variant="filter-group">
                <ToolbarItem key="start-date">
                  <DatePicker
                    value={startDateValue}
                    onBlur={(_event, str, date) => onStartTimeChange(date)}
                    onChange={(_event, str, date) => onStartTimeChange(date)}
                    helperText="Start date"
                  />
                </ToolbarItem>
                &nbsp;
                <ToolbarItem key="end-date">
                  <DatePicker
                    value={endDateValue}
                    onBlur={(_event, str, date) => onEndTimeChange(date)}
                    onChange={(_event, str, date) => onEndTimeChange(date)}
                    helperText="End date"
                  />
                </ToolbarItem>
                <ToolbarItem
                  key="preset"
                  style={{flexDirection: "column", marginLeft: "4px"}}>
                  <Dropdown
                    onSelect={onPresetChange}
                    position={DropdownPosition.left}
                    toggle={
                      <DropdownToggle
                        onToggle={(open) => setIsPresetOpen(open)}
                        style={{ width: '100%' }}
                      >
                        Preset: {currentPreset}
                      </DropdownToggle>
                    }
                    isOpen={isPresetOpen}
                    dropdownItems={presets.map((preset) => (
                      <DropdownItem key={preset[1]}>{preset[1]}</DropdownItem>
                    ))}
                    style={{ width: '100%' }}
                    menuAppendTo={document.body}
                    helperText="Preset"
                  /><br/>
                  <HelperText style={{marginTop: "4px"}}>
                    <HelperTextItem>
                      &nbsp;
                  </HelperTextItem>
                  </HelperText>
                </ToolbarItem>
              </ToolbarGroup>
            </ToolbarToggleGroup>
          </ToolbarContent>
        </Toolbar>
        <>
          {renderBuilds(buildList)}
        </>
      </PageSection>
    </>
  )
}

RuntimePage.propTypes = {
  tenant: PropTypes.object,
  preferences: PropTypes.object,
  timezone: PropTypes.string,
}

function mapStateToProps(state) {
  return {
    tenant: state.tenant,
    preferences: state.preferences,
    timezone: state.timezone,
  }
}

export default connect(mapStateToProps)(RuntimePage)
