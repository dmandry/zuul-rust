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

import React, { useEffect, useState, useCallback } from 'react'
import PropTypes from 'prop-types'
import { connect, useDispatch } from 'react-redux'
import { useHistory, useLocation } from 'react-router-dom'
import {
  PageSection,
  PageSectionVariants,
  Text,
  TextContent,
  Select,
  SelectOption,
  SelectVariant,
  Form,
  FormGroup,
  TextInput,
  Checkbox,
  ActionGroup,
  Button,
} from '@patternfly/react-core'

import { enqueue_ref, freezePipeline } from '../api'
import { addNotification, addApiError } from '../actions/notifications'

import FreezePipelineToolbar from '../containers/enqueue/FreezePipelineToolbar'

function EnqueuePage(props) {
  const { tenant } = props

  const dispatch = useDispatch()
  const [currentPipeline, setCurrentPipeline] = useState()
  const [currentProject, setCurrentProject] = useState()
  const [currentRefType, setCurrentRefType] = useState()
  const [currentRef, setCurrentRef] = useState()
  const [parameterList, setParameterList] = useState()
  const history = useHistory()
  const location = useLocation()
  const [paramState, setParamState] = useState({})

  if (!currentRef) {
    const urlParams = new URLSearchParams(location.search)
    const pipeline = urlParams.get('pipeline')
    const project = urlParams.get('project')
    const refType = urlParams.get('ref_type')
    const ref = urlParams.get('ref')
    if (pipeline && refType && ref && project) {
      setCurrentPipeline(pipeline)
      setCurrentProject(project)
      setCurrentRefType(refType)
      setCurrentRef(ref)
    }
  }

  const updateData = useCallback(() => {
    const ref = (currentRefType === 'Branch') ?
          `refs/heads/${currentRef}` : (currentRefType === 'Tag') ?
          `refs/tags/${currentRef}` : undefined

    freezePipeline(tenant.apiPrefix, currentPipeline, currentProject, ref).then((response) => {
      setParameterList(response.data.parameters)
    })
  }, [currentPipeline, currentProject,
      currentRefType, currentRef, tenant.apiPrefix])

  useEffect(() => {
    document.title = 'Zuul Enqueue'
    if (currentPipeline && currentProject && currentRefType && currentRef) {
      updateData()
    }
  }, [updateData, tenant, currentPipeline, currentProject, currentRefType, currentRef, dispatch])

  function onChange(pipeline, project, refType, ref) {
    setCurrentPipeline(pipeline)
    setCurrentProject(project)
    setCurrentRefType(refType)
    setCurrentRef(ref)

    const searchParams = new URLSearchParams('')
    searchParams.append('pipeline', pipeline)
    searchParams.append('project', project)
    searchParams.append('ref_type', refType)
    searchParams.append('ref', ref)
    history.push({
      pathname: location.pathname,
      search: searchParams.toString(),
    })
  }

  const getParamState = useCallback(((param, key) => {
    if (paramState[param.name] === undefined) {
      return undefined
    }
    return paramState[param.name][key]
  }), [paramState])

  function updateParamState(param, update) {
    setParamState(prevState => ({
      ...prevState,
      [param.name]: {...prevState[param.name], ...update}
    }))
  }

  function renderSelection(param) {
    return (
      <FormGroup
        label={param.name}
        helperText={param.description}
        isRequired={param.required}
      >
        <Select
          id={param.name}
          isOpen={getParamState(param, 'isOpen')}
          validated={getParamState(param, 'validated')}
          onToggle={(isOpen) =>
            updateParamState(param, {'isOpen': isOpen})
          }
          selections={getParamState(param, 'value')}
          onSelect={(event, selection) =>
            updateParamState(param, {'isOpen': false, 'value': selection})
          }
        >
          {param.values.map((value, index) => (
            <SelectOption key={index} value={value} label={value}/>
          ))}
        </Select>
      </FormGroup>
    )
  }

  function renderMultipleSelection(param) {

    function onSelect(event, selection) {
      const prev = getParamState(param, 'value')
      const selected = prev === undefined ? [] : prev
      if (selected && selected.includes(selection)) {
        updateParamState(param, {
          'isOpen': false,
          'value': selected.filter(x => x !== selection)
        })
      } else {
        updateParamState(param, {
          'isOpen': false,
          'value': [...selected, selection],
        })
      }
    }

    return (
      <FormGroup
        label={param.name}
        helperText={param.description}
        isRequired={param.required}
      >
        <Select
          id={param.name}
          variant={SelectVariant.typeaheadMulti}
          validated={getParamState(param, 'validated')}
          isOpen={getParamState(param, 'isOpen')}
          onToggle={(isOpen) =>
            updateParamState(param, {'isOpen': isOpen})
          }
          selections={getParamState(param, 'value')}
          onSelect={onSelect}
        >
          {param.values.map((value, index) => (
            <SelectOption key={index} value={value} label={value}/>
          ))}
        </Select>
      </FormGroup>
    )
  }

  function renderText(param) {
    return (
      <FormGroup
        label={param.name}
        helperText={param.description}
        isRequired={param.required}
      >
        <TextInput
          id={param.name}
          type="text"
          value={getParamState(param, 'value')}
          validated={getParamState(param, 'validated')}
          onChange={(value) =>
            updateParamState(param, {'value': value})
          }
        />
      </FormGroup>
    )
  }

  function renderCheckbox(param) {
    return (
      <FormGroup
        helperText={param.description}
      >
        <Checkbox
          id={param.name}
          label={param.name}
          isChecked={getParamState(param, 'value')}
          onChange={(value) =>
            updateParamState(param, {'value': value})
          }
        />
      </FormGroup>
    )
  }

  function renderParameters(parameters) {
    const widgets = []
    const loop = parameters ? parameters : []
    loop.forEach((param) => {
      if (param.default !== null &&
          getParamState(param, 'value') === undefined) {
        updateParamState(param, {'value': param.default})
      }
      if (param.type === 'selection') {
        widgets.push(renderSelection(param))
      } else if (param.type === 'multiple-selection') {
        widgets.push(renderMultipleSelection(param))
      } else if (param.type === 'string') {
        widgets.push(renderText(param))
      } else if (param.type === 'bool') {
        widgets.push(renderCheckbox(param))
      }
    })
    return widgets
  }

  const submitEnqueue = useCallback(() => {
    const parameters = {}
    const loop = parameterList ? parameterList : []
    let error = false
    loop.forEach((param) => {
      const val = getParamState(param, 'value')
      if (param.required && !val) {
        error = true
        updateParamState(param, {'validated': 'error'})
      } else {
        updateParamState(param, {'validated': 'default'})
      }
      parameters[param.name] = val
    })
    if (!error) {
        const ref = (currentRefType === 'Branch') ?
              `refs/heads/${currentRef}` : (currentRefType === 'Tag') ?
              `refs/tags/${currentRef}` : undefined

        enqueue_ref(tenant.apiPrefix, currentProject, currentPipeline,
                    ref, null, null, parameters).then(() => {
                      dispatch(addNotification(
                        {
                          text: 'Enqueue successful.',
                          type: 'success',
                          status: '',
                          url: '',
                        }))
                    })
          .catch(error => {
            dispatch(addApiError(error))
          })
    }
  }, [currentPipeline, currentProject, getParamState,
      currentRefType, currentRef, tenant.apiPrefix, parameterList, dispatch])

  const parameterWidgets = renderParameters(parameterList)

  return (
    <>
      <PageSection variant={PageSectionVariants.light}>
        <TextContent>
          <Text component="h1">Enqueue</Text>
        </TextContent>
        <FreezePipelineToolbar
          onChange={onChange}
          defaultPipeline={currentPipeline}
          defaultProject={currentProject}
          defaultRefType={currentRefType}
          defaultRef={currentRef}
          buttonText="Fetch"
        />
        <Form style={{maxWidth: "40em"}}>
          {parameterWidgets}
          <ActionGroup>
            <Button
              variant="primary"
              onClick={submitEnqueue}
            >
              Enqueue
            </Button>
          </ActionGroup>
        </Form>
      </PageSection>
    </>
  )
}

EnqueuePage.propTypes = {
  tenant: PropTypes.object,
}

function mapStateToProps(state) {
  return {
    tenant: state.tenant,
  }
}

export default connect(mapStateToProps)(EnqueuePage)
