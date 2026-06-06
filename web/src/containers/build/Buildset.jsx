// Copyright 2019 Red Hat, Inc
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
import { connect, useDispatch } from 'react-redux'
import { Link } from 'react-router-dom'
import { JSONTree } from 'react-json-tree'
import {
  Button,
  CodeBlock,
  CodeBlockCode,
  ExpandableSection,
  ExpandableSectionToggle,
  Flex,
  FlexItem,
  List,
  ListItem,
  Title,
  Modal,
  ModalVariant,
} from '@patternfly/react-core'
import {
  CodeIcon,
  CodeBranchIcon,
  OutlinedCommentDotsIcon,
  FingerprintIcon,
  StreamIcon,
  OutlinedCalendarAltIcon,
  OutlinedClockIcon,
  RedoAltIcon,
} from '@patternfly/react-icons'
import momentDurationSetup from 'moment-duration-format'
import moment from 'moment'
import moment_tz from 'moment-timezone'

import {
  buildExternalLink,
  isAuthorized,
  renderRefInfo,
} from '../../Misc'
import { IconProperty } from '../../MiscComponents'
import { BuildResultBadge, BuildResultWithIcon } from './Misc'
import { enqueue, enqueue_ref } from '../../api'
import { addNotification, addApiError } from '../../actions/notifications'
import { ChartModal } from '../charts/ChartModal'
import BuildsetGanttChart from '../charts/GanttChart'

momentDurationSetup(moment_tz)

const BUILDSET_MESSAGE_PREVIEW_LINES = 8

function getRefs(buildset) {
  // For backwards compat: get a list of this items changes.
  return 'refs' in buildset ? buildset.refs : [buildset]
}

function getRef(buildset) {
  return 'refs' in buildset ? buildset.refs[0] : buildset
}

function Buildset({ buildset, timezone, tenant, user, preferences }) {
  const [isGanttChartModalOpen, setIsGanttChartModalOpen] = useState(false)
  const [isMessageExpanded, setIsMessageExpanded] = useState(false)
  const ref = getRef(buildset)

  function onMessageToggle(isExpanded) {
    setIsMessageExpanded(isExpanded)
  }

  function formatMessage() {
    const lines = buildset.message ? buildset.message.split('\n') : []
    if (lines.length > BUILDSET_MESSAGE_PREVIEW_LINES) {
      const chunk = BUILDSET_MESSAGE_PREVIEW_LINES
      const lines_preview = lines.slice(0, chunk)
      const lines_expanded = lines.slice(chunk, chunk + lines.length)
      return [lines_preview.join('\n'), lines_expanded.join('\n')]
    }
    return [buildset.message, null]
  }

  const [message, expandedMessage] = formatMessage()

  function renderBuildTimes() {
    const firstStartBuild = buildset.builds.reduce((prev, cur) =>
      !cur.start_time || prev.start_time < cur.start_time ? prev : cur
    )
    const lastEndBuild = buildset.builds.reduce((prev, cur) =>
      !cur.end_time || prev.end_time > cur.end_time ? prev : cur
    )
    const totalDuration =
      (moment_tz.utc(lastEndBuild.end_time).tz(timezone) -
        moment_tz.utc(firstStartBuild.start_time).tz(timezone)) /
      1000
    const overallDuration =
      (moment_tz.utc(lastEndBuild.end_time).tz(timezone) -
        moment_tz.utc(
          buildset.event_timestamp != null
            ? buildset.event_timestamp : firstStartBuild.start_time
        ).tz(timezone)
      ) / 1000

    const buildLink = (build) => (
      <Link to={`${tenant.linkPrefix}/build/${build.uuid}`}>
        {build.job_name}
      </Link>
    )
    const firstStartLink = buildLink(firstStartBuild)
    const lastEndLink = buildLink(lastEndBuild)

    return (
      <Flex flex={{ default: 'flex_1' }}>
        <FlexItem>
          <List style={{ listStyle: 'none' }}>
            <IconProperty
              WrapElement={ListItem}
              icon={<OutlinedCalendarAltIcon />}
              value={
                <span>
                  <strong>Starting build </strong>
                  {firstStartLink} <br />
                  <i>
                    (started{' '}
                    {moment_tz
                      .utc(firstStartBuild.start_time)
                      .tz(timezone)
                      .format('YYYY-MM-DD HH:mm:ss')}
                    )
                  </i>
                  <br />
                  <strong>Ending build </strong>
                  {lastEndLink} <br />
                  <i>
                    (ended{' '}
                    {moment_tz
                      .utc(lastEndBuild.end_time)
                      .tz(timezone)
                      .format('YYYY-MM-DD HH:mm:ss')}
                    )
                  </i>
                </span>
              }
            />
            <IconProperty
              WrapElement={ListItem}
              icon={<OutlinedClockIcon />}
              value={
                <>
                  <strong>Buildset duration </strong>
                  {moment
                    .duration(totalDuration, 'seconds')
                    .format('h [hr] m [min] s [sec]')}{' '}
                  &nbsp;
                  <Button
                    key="GanttChartToggle"
                    variant="secondary"
                    onClick={() => {
                      setIsGanttChartModalOpen(true)
                    }}
                  >
                    Show timeline
                  </Button>
                </>
              }
            />
            <IconProperty
              WrapElement={ListItem}
              icon={<OutlinedClockIcon />}
              value={
                <>
                  <strong>Overall duration </strong>
                  {moment
                    .duration(overallDuration, 'seconds')
                    .format('h [hr] m [min] s [sec]')}
                </>
              }
            />
          </List>
        </FlexItem>
      </Flex>
    )
  }

  const [showEnqueueModal, setShowEnqueueModal] = useState(false)
  const dispatch = useDispatch()

  function renderEnqueueButton() {
    const value = (<span style={{
      cursor: 'pointer',
      color: 'var(--pf-global--primary-color--100)'
    }}
      title="Re-enqueue this change"
      onClick={(event) => {
        event.preventDefault()
        setShowEnqueueModal(true)
      }}
    >
      Re-enqueue buildset
    </span>)
    return (
      <IconProperty
        WrapElement={ListItem}
        icon={<RedoAltIcon />}
        value={value}
      />
    )
  }

  function enqueueConfirm() {
    setShowEnqueueModal(false)
    if (ref.change === null) {
      enqueue_ref(tenant.apiPrefix, ref.project, buildset.pipeline,
                  ref.ref, ref.oldrev, ref.newrev, buildset.parameters)
        .then(() => {
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
    } else {
      const changeId = ref.change + ',' + ref.patchset
      enqueue(tenant.apiPrefix, ref.project, buildset.pipeline, changeId)
        .then(() => {
          dispatch(addNotification(
            {
              text: 'Change enqueued successfully.',
              type: 'success',
              status: '',
              url: '',
            }))
        })
        .catch(error => {
          dispatch(addApiError(error))
        })
    }
  }

  function renderEnqueueModal() {
    let changeId = ref.change ? ref.change + ',' + ref.patchset : ref.newrev
    let changeInfo = changeId
      ? <>for change <strong>{changeId}</strong></>
      : <>for ref <strong>{ref.ref}</strong></>
    const title = 'You are about to re-enqueue a change'
    return (
      <Modal
        variant={ModalVariant.small}
        // titleIconVariant={BullhornIcon}
        isOpen={showEnqueueModal}
        title={title}
        onClose={() => { setShowEnqueueModal(false) }}
        actions={[
          <Button key="deq_confirm" variant="primary" onClick={enqueueConfirm}>Confirm</Button>,
          <Button key="deq_cancel" variant="link" onClick={() => { setShowEnqueueModal(false) }}>Cancel</Button>,
        ]}>
        <p>Please confirm that you want to re-enqueue <strong>all jobs</strong> {changeInfo} on project <strong>{ref.project}</strong> on pipeline <strong>{buildset.pipeline}</strong>.</p>
      </Modal>
    )
  }

  function renderEvents() {
    return (
      <>
        {buildset.events.map((bs_event, idx) => (
          <IconProperty
            WrapElement={ListItem}
            icon={<OutlinedClockIcon />}
            key={idx}
            value={
              <span>
                {bs_event.description} <br />
                <i>
                  {moment_tz
                   .utc(bs_event.event_time)
                   .tz(timezone)
                   .format('YYYY-MM-DD HH:mm:ss')}
                </i>
              </span>
            }
          />
        ))}
      </>
    )
  }

  return (
    <>
      <Title headingLevel="h2">
        <BuildResultWithIcon result={buildset.result} size="md">
          Buildset result
        </BuildResultWithIcon>
        <BuildResultBadge result={buildset.result} /> &nbsp;
      </Title>
      {/* We handle the spacing for the body and the flex items by ourselves
            so they go hand in hand. By default, the flex items' spacing only
            affects left/right margin, but not top or bottom (which looks
            awkward when the items are stacked at certain breakpoints) */}
      <Flex className="zuul-build-attributes">
        <Flex flex={{ default: 'flex_1' }}>
          <FlexItem>
            <List style={{ listStyle: 'none' }}>
              {getRefs(buildset).map((ref, idx) => (
                <IconProperty
                  WrapElement={ListItem}
                  icon={<CodeBranchIcon />}
                  key={idx}
                  value={
                    <span>
                      {buildExternalLink(ref)}<br/>
                      <strong>Project </strong> {ref.project}<br/>
                      {renderRefInfo(ref)}
                    </span>
                  }
                />
              ))}
              <IconProperty
                WrapElement={ListItem}
                icon={<StreamIcon />}
                value={
                  <>
                    <strong>Pipeline </strong> {buildset.pipeline}
                  </>
                }
              />
              <IconProperty
                WrapElement={ListItem}
                icon={<FingerprintIcon />}
                value={
                  <span>
                    <strong>UUID </strong> {buildset.uuid} <br />
                    <strong>Event ID </strong> {buildset.event_id} <br />
                    {buildset.user_id && (
                      <>
                        <strong>User ID </strong> {buildset.user_id} <br />
                      </>
                    )}
                  </span>
                }
              />
              {buildset.parameters && (
                <IconProperty
                  WrapElement={ListItem}
                  icon={<CodeIcon />}
                  value={
                    <>
                      <strong>Parameters </strong>
                      <span style={{whiteSpace: 'pre-wrap'}}>
                        <JSONTree
                          data={buildset.parameters}
                          hideRoot={true}
                          sortObjectKeys={true}
                          theme="default"
                          invertTheme={!preferences.darkMode}
                          shouldExpandNodeInitially={() => true}
                        />
                      </span>
                    </>
                  }
                />)}
            </List>
          </FlexItem>
        </Flex>
        {buildset.builds && renderBuildTimes()}
        <Flex flex={{ default: 'flex_1' }}>
          <FlexItem>
            <List style={{ listStyle: 'none' }}>
              {buildset.events && renderEvents()}
              <IconProperty
                WrapElement={ListItem}
                icon={<OutlinedCommentDotsIcon />}
                value={
                  <>
                    <strong>Message:</strong>
                    <div className={preferences.darkMode ? 'zuul-console-dark' : ''}>
                      <CodeBlock>
                        <CodeBlockCode>
                          {message}
                          {expandedMessage ?
                            <ExpandableSection
                              isExpanded={isMessageExpanded}
                              isDetached
                              contentId="message-expand"
                            >
                              {expandedMessage}
                            </ExpandableSection> : ''}
                        </CodeBlockCode>
                        {expandedMessage ?
                          <ExpandableSectionToggle
                            isExpanded={isMessageExpanded}
                            onToggle={onMessageToggle}
                            contentId="message-expand"
                            direction="up"
                          >
                            {isMessageExpanded ? 'Show Less' : 'Show More'}
                          </ExpandableSectionToggle> : ''}
                      </CodeBlock>
                    </div>
                  </>
                }
              />
              {isAuthorized(user, 'enqueue') &&
               <>
                 {renderEnqueueButton()}
               </>}
            </List>
          </FlexItem>
      </Flex>

      </Flex>
      <ChartModal
        chart={<BuildsetGanttChart builds={buildset.builds} />}
        isOpen={isGanttChartModalOpen}
        title="Builds Timeline"
        onClose={() => {
          setIsGanttChartModalOpen(false)
        }}
      />
      {renderEnqueueModal()}
    </>
  )
}

Buildset.propTypes = {
  buildset: PropTypes.object,
  tenant: PropTypes.object,
  timezone: PropTypes.string,
  user: PropTypes.object,
  preferences: PropTypes.object,
}

export default connect((state) => ({
  tenant: state.tenant,
  timezone: state.timezone,
  user: state.user,
  preferences: state.preferences,
}))(Buildset)
