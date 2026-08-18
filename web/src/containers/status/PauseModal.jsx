// Copyright 2025 Acme Gating, LLC
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
import PropTypes from 'prop-types'
import { useDispatch, useSelector } from 'react-redux'

import {
  Button,
  FormGroup,
  Modal,
  ModalVariant,
  Radio,
  TextInput,
} from '@patternfly/react-core'

import { addApiError } from '../../actions/notifications'
import { setTenantState } from '../../api'

function PauseModal({isOpen, setOpen}) {
  const tenant = useSelector((state) => state.tenant)
  const [reason, setReason] = useState('')
  const [info, setInfo] = useState('')
  const [queueState, setQueueState] = useState('normal')
  const [discardTrigger, setDiscardTrigger] = useState(false)
  const [pauseTrigger, setPauseTrigger] = useState(false)
  const [pauseResult, setPauseResult] = useState(false)
  const dispatch = useDispatch()

  return (
    <Modal
      variant={ModalVariant.small}
      isOpen={isOpen}
      title="Manage Tenant Event Processing"
      onClose={() => { setOpen(false) }}
      actions={[
        <Button key="confirm" variant="primary"
                onClick={() => {
                  setOpen(false)
                  setTenantState(
                    tenant.apiPrefix,
                    discardTrigger,
                    pauseTrigger,
                    pauseResult,
                    reason,
                    info)
                    .catch(error => {
                      dispatch(addApiError(error))
                    })
                }}>
          Confirm
        </Button>,
        <Button key="cancel" variant="link"
                onClick={() => { setOpen(false) }}>
          Cancel</Button>,
      ]}>
      <p>You can pause trigger or result event processing for this tenant, or discard trigger events.  Trigger events cause new items to appear in pipelines.  Result events cause item results to be reported (and potentially, changes merged).</p>

      <FormGroup
        label="Pause"
        fieldId="pause-form-unpaused">
        <Radio
          id="pause-form-unpaused"
          label="Unpaused"
          isChecked={queueState === 'normal'}
          onChange={() => {
            setQueueState('normal')
            setDiscardTrigger(false)
            setPauseTrigger(false)
            setPauseResult(false)
          }}
        />
        <Radio
          id="pause-form-trigger-queue-paused"
          label="Pause trigger queue"
          isChecked={queueState === 'pause-trigger'}
          onChange={() => {
            setQueueState('pause-trigger')
            setDiscardTrigger(false)
            setPauseTrigger(true)
            setPauseResult(false)
          }}
        />
        <Radio
          id="pause-form-result-queue-paused"
          label="Pause trigger and result queues"
          isChecked={queueState === 'pause-result'}
          onChange={() => {
            setQueueState('pause-result')
            setDiscardTrigger(false)
            setPauseTrigger(true)
            setPauseResult(true)
          }}
        />
        <Radio
          id="pause-form-trigger-discard"
          label="Discard trigger events"
          isChecked={queueState === 'discard-trigger'}
          onChange={() => {
            setQueueState('discard-trigger')
            setDiscardTrigger(true)
            setPauseTrigger(false)
            setPauseResult(false)
          }}
        />
      </FormGroup>
      <FormGroup
        label="Reason"
        fieldId="pause-form-reason"
        helperText="This explanation will appear on the status page while event processing is inactive.">
        <TextInput
          value={reason}
          isRequired
          type="text"
          id="pause-form-reason"
          name="pauseReason"
          onChange={(value) => { setReason(value) }}
        />
      </FormGroup>
      <FormGroup
        label="Info"
        fieldId="pause-form-info"
        helperText="This text will appear in a separate info banner on the status page, regardless of event processing status.">
        <TextInput
          value={info}
          type="text"
          id="pause-form-info"
          name="pauseInfo"
          onChange={(value) => { setInfo(value) }}
        />
      </FormGroup>
    </Modal>
  )
}

PauseModal.propTypes = {
  isOpen: PropTypes.bool,
  setOpen: PropTypes.func,
}

export default (PauseModal)
