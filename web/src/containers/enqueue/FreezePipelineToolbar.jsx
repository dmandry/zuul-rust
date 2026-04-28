// Copyright 2020 BMW Group
// Copyright 2022, 2026 Acme Gating, LLC
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

import React, { useEffect, useState } from 'react'
import { connect } from 'react-redux'
import PropTypes from 'prop-types'
import {
  Button,
  TextInput,
  Dropdown,
  DropdownItem,
  DropdownPosition,
  DropdownToggle,
  Toolbar,
  ToolbarContent,
  ToolbarGroup,
  ToolbarItem,
  ToolbarToggleGroup,
} from '@patternfly/react-core'

import { fetchPipelinesIfNeeded } from '../../actions/pipelines'

function FreezePipelineToolbar(props) {
  const { tenant, fetchPipelinesIfNeeded } = props

  useEffect(() => {
    fetchPipelinesIfNeeded(tenant)
  }, [fetchPipelinesIfNeeded, tenant])

  const tenantPipelines = props.pipelines.pipelines[tenant.name]
  const pipelines = tenantPipelines ? tenantPipelines.map(p => { return p.name }) : []

  const [isPipelineOpen, setIsPipelineOpen] = useState(false)
  const [isRefTypeOpen, setIsRefTypeOpen] = useState(false)
  const [currentPipeline, setCurrentPipeline] = useState(props.defaultPipeline || '')
  const [currentProject, setCurrentProject] = useState(props.defaultProject || '')
  const [currentRefType, setCurrentRefType] = useState(props.defaultRefType || 'Branch')
  const [currentRef, setCurrentRef] = useState(props.defaultRef || '')

  if (!currentPipeline && pipelines.length) {
    // We may have gotten a list of pipelines after we loaded the page
    setCurrentPipeline(pipelines[0])
  }

  function handlePipelineSelect(event) {
    setCurrentPipeline(event.target.innerText)
    setIsPipelineOpen(false)
  }

  function handlePipelineToggle(isOpen) {
    setIsPipelineOpen(isOpen)
  }

  function handleRefTypeSelect(event) {
    setCurrentRefType(event.target.innerText)
    setIsRefTypeOpen(false)
  }

  function handleRefTypeToggle(isOpen) {
    setIsRefTypeOpen(isOpen)
  }

  function handleProjectChange(newValue) {
    setCurrentProject(newValue)
  }

  function handleRefChange(newValue) {
    setCurrentRef(newValue)
  }

  function handleInputSend(event) {
    // In case the event comes from a key press, only accept "Enter"
    if (event.key && event.key !== 'Enter') {
      return
    }

    // Ignore empty values
    if (!currentRef || !currentProject) {
      return
    }

    // Notify the parent component about the filter change
    props.onChange(currentPipeline, currentProject, currentRefType, currentRef)
  }

  function renderFreezePipelineToolbar () {
    const buttonText = props.buttonText || 'Freeze Pipeline'
    return <>
      <Toolbar collapseListedFiltersBreakpoint="md">
        <ToolbarContent>
          <ToolbarToggleGroup breakpoint="md">
            <ToolbarGroup variant="filter-group">

              <ToolbarItem key="pipeline">
                <Dropdown
                  onSelect={handlePipelineSelect}
                  position={DropdownPosition.left}
                  toggle={
                    <DropdownToggle
                      onToggle={handlePipelineToggle}
                      style={{ width: '100%' }}
                    >
                      Pipeline: {currentPipeline}
                    </DropdownToggle>
                  }
                  isOpen={isPipelineOpen}
                  dropdownItems={pipelines.map((pipeline) => (
                    <DropdownItem key={pipeline}>{pipeline}</DropdownItem>
                  ))}
                  style={{ width: '100%' }}
                  menuAppendTo={document.body}
                />
              </ToolbarItem>

              <ToolbarItem key="project">
                <TextInput
                  name="project"
                  id="project-input"
                  type="search"
                  placeholder="Project"
                  defaultValue={props.defaultProject}
                  onChange={handleProjectChange}
                  onKeyDown={(event) => handleInputSend(event)}
                />
              </ToolbarItem>

              <ToolbarItem key="ref_type">
                <Dropdown
                  onSelect={handleRefTypeSelect}
                  position={DropdownPosition.left}
                  toggle={
                    <DropdownToggle
                      onToggle={handleRefTypeToggle}
                      style={{ width: '100%' }}
                    >
                      {currentRefType}
                    </DropdownToggle>
                  }
                  isOpen={isRefTypeOpen}
                  dropdownItems={[
                    <DropdownItem key="Branch">Branch</DropdownItem>,
                    <DropdownItem key="Tag">Tag</DropdownItem>,
                  ]}
                  style={{ width: '100%' }}
                  menuAppendTo={document.body}
                />
              </ToolbarItem>

              <ToolbarItem key="ref">
                <TextInput
                  name="ref"
                  id="ref-input"
                  type="search"
                  placeholder={`${currentRefType} name`}
                  defaultValue={props.defaultRef}
                  onChange={handleRefChange}
                  onKeyDown={(event) => handleInputSend(event)}
                />
              </ToolbarItem>

              <ToolbarItem key="button">
                <Button
                  onClick={(event) => handleInputSend(event)}
                >
                  {buttonText}
                </Button>
              </ToolbarItem>

            </ToolbarGroup>
          </ToolbarToggleGroup>
        </ToolbarContent>
      </Toolbar>
    </>
  }

  return (
    <div>
      {renderFreezePipelineToolbar()}
    </div>
  )
}

FreezePipelineToolbar.propTypes = {
  fetchPipelinesIfNeeded: PropTypes.func,
  tenant: PropTypes.object,
  pipelines: PropTypes.object,
  onChange: PropTypes.func.isRequired,
  defaultPipeline: PropTypes.string,
  defaultProject: PropTypes.string,
  defaultRef: PropTypes.string,
  defaultRefType: PropTypes.string,
  buttonText: PropTypes.string,
}

function mapStateToProps(state) {
  return {
    tenant: state.tenant,
    pipelines: state.pipelines,
  }
}

const mapDispatchToProps = {
  fetchPipelinesIfNeeded
}

export default connect(mapStateToProps, mapDispatchToProps)(FreezePipelineToolbar)
