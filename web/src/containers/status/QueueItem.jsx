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

import React, { useState } from 'react'
import PropTypes from 'prop-types'
import { useHistory, useLocation } from 'react-router-dom'
import { connect, useDispatch, useSelector } from 'react-redux'

import {
  Button,
  Card,
  CardActions,
  CardTitle,
  CardBody,
  CardHeader,
  DataList,
  DataListCell,
  DataListItem,
  DataListItemCells,
  DataListItemRow,
  DataListToggle,
  Dropdown,
  DropdownItem,
  DropdownToggle,
  ExpandableSection,
  KebabToggle,
  Modal,
  ModalVariant,
} from '@patternfly/react-core'
import {
  AngleDoubleUpIcon,
  BanIcon,
  FilterIcon,
} from '@patternfly/react-icons'

import {
  calculateQueueItemTimes,
  getChangeLabel,
  getJobStrResult,
  getRefs,
} from './Misc'
import {
  ChangeLink,
  JobLink,
  JobResultOrStatus,
} from './MiscComponents'
import { isAuthorized } from '../../Misc'

import QueueItemProgress from './QueueItemProgress'

import { dequeue, dequeue_ref, promote } from '../../api'
import { addDequeueError, addPromoteError } from '../../actions/adminActions'
import { addNotification } from '../../actions/notifications'
import { fetchStatusIfNeeded } from '../../actions/status'
import { expandJobs, collapseJobs } from '../../actions/statusExpansion'

function FilterDropdown({ item, pipeline }) {
  const [isFilterDropdownOpen, setIsFilterDropdownOpen] = useState(false)

  const history = useHistory()
  const location = useLocation()
  const ref = getRefs(item)[0]

  const onFilterDropdownSelect = () => {
    setIsFilterDropdownOpen(!isFilterDropdownOpen)
  }

  const onSelect = () => {
    setIsFilterDropdownOpen(false)
  }

  const applyFilters = (filters) => {
    history.push({
      pathname: location.pathname,
      search: filters.toString(),
    })
  }

  const filterChange = () => {
    const filterParams = new URLSearchParams('')
    filterParams.append('change', ref.id || ref.ref)
    applyFilters(filterParams)
  }

  const filterProject = () => {
    const filterParams = new URLSearchParams('')
    filterParams.append('project', ref.project)
    applyFilters(filterParams)
  }

  const filterItem = () => {
    const filterParams = new URLSearchParams('')
    filterParams.append('change', ref.id || ref.ref)
    filterParams.append('pipeline', pipeline.name)
    applyFilters(filterParams)
  }

  const changeText = getChangeLabel(ref)

  const filterDropdownItems = [
    <DropdownItem
      key="filter-change"
      description={`Filter change: ${changeText}`}
      onClick={() => filterChange()}
    >
      Filter by change
    </DropdownItem>,
    <DropdownItem
      key="filter-project"
      description={`Filter project: ${ref.project}`}
      onClick={() => filterProject()}
    >
      Filter by project
    </DropdownItem>,
    <DropdownItem
      key="filter-item"
      description={`Filter item: ${changeText} in ${pipeline.name}`}
      onClick={() => filterItem()}
    >
      Filter by item
    </DropdownItem>
  ]

  return (
    <Dropdown
      className="zuul-change-filter-dropdown"
      position="right"
      onSelect={onSelect}
      toggle={
        <DropdownToggle
          toggleIndicator={null}
          onToggle={onFilterDropdownSelect}
          aria-label="Change filters"
          id={`toggle-change-filters-${ref.id || ref.ref}`}
        >
          <FilterIcon />
        </DropdownToggle>
      }
      isOpen={isFilterDropdownOpen}
      isPlain
      dropdownItems={filterDropdownItems}
    />
  )
}

FilterDropdown.propTypes = {
  item: PropTypes.object.isRequired,
  pipeline: PropTypes.object.isRequired,
}

function JobRow({ job, idx, tenant, job_times, className }) {
  return (
    <DataListItem key={idx} className={className}>
      <DataListItemRow>
        <DataListItemCells
          dataListCells={[
            <DataListCell key={`${job.name}-name`}>
              <JobLink job={job} tenant={tenant} />
            </DataListCell>,
            <DataListCell isFilled={false} alignRight key={`${job.name}-result`}>
              {/* TODO (felix): Since the job.name is not unique anymore,
                      this should be looked up by job.uuid */}
              <JobResultOrStatus job={job} job_times={job_times[job.name]} />
            </DataListCell>
          ]}
        />
      </DataListItemRow>
    </DataListItem>
  )
}

JobRow.propTypes = {
  job: PropTypes.object.isRequired,
  idx: PropTypes.number.isRequired,
  tenant: PropTypes.object.isRequired,
  job_times: PropTypes.object.isRequired,
  className: PropTypes.string,
}

function SkippedJobList({ skippedJobs, isExpanded, onToggle, tenant, job_times }) {
  return (
    <>
      <DataListItem key="skip-jobs" isExpanded={isExpanded}>
        <DataListItemRow>
          <DataListToggle
            onClick={() => onToggle()}
            isExpanded={isExpanded}
            id="expand-skipped-jobs"
            aria-controls="expand-skipped-jobs"
          />
          <DataListItemCells
            dataListCells={[
              <DataListCell key="show skipped jobs">
                <div>{`Show ${skippedJobs.length} skipped jobs`}</div>
              </DataListCell>
            ]}
          />
        </DataListItemRow>
      </DataListItem>
      {isExpanded ?
        skippedJobs.map((job, idx) => (
          <JobRow
            key={idx}
            job={job}
            idx={idx}
            tenant={tenant}
            job_times={job_times}
            className={'zuul-skipped-job-row'}
          />
        ))
        : ''}
    </>
  )
}

SkippedJobList.propTypes = {
  skippedJobs: PropTypes.array.isRequired,
  isExpanded: PropTypes.bool.isRequired,
  onToggle: PropTypes.func.isRequired,
  tenant: PropTypes.object.isRequired,
  job_times: PropTypes.object.isRequired,
}

function JobList({
  jobs, skippedJobs, isSkippedJobsExpanded, onSkippedJobsToggle, tenant, job_times
}) {
  return (
    <DataList isCompact className="zuul-job-list">
      {jobs.map((job, idx) => (
        <JobRow key={idx} job={job} idx={idx} tenant={tenant} job_times={job_times} />
      ))}
      {skippedJobs.length > 0 ?
        <SkippedJobList
          skippedJobs={skippedJobs}
          isExpanded={isSkippedJobsExpanded}
          onToggle={onSkippedJobsToggle}
          tenant={tenant}
          job_times={job_times}
        />
        : ''}
    </DataList>
  )
}

JobList.propTypes = {
  jobs: PropTypes.array.isRequired,
  skippedJobs: PropTypes.array.isRequired,
  isSkippedJobsExpanded: PropTypes.bool.isRequired,
  onSkippedJobsToggle: PropTypes.func.isRequired,
  tenant: PropTypes.object.isRequired,
  job_times: PropTypes.object.isRequired,
}

function DequeueModal({ item, isOpen, onConfirm, onCancel }) {
  const ref = getRefs(item)[0]
  let projectName = ref.project
  let refId = ref.id || ref.ref
  return (
    <Modal
      variant={ModalVariant.small}
      isOpen={isOpen}
      title="You are about to dequeue a change"
      onClose={onCancel}
      actions={[
        <Button key="deq_confirm" variant="primary" onClick={onConfirm}>
          Confirm
        </Button>,
        <Button key="deq_cancel" variant="link" onClick={onCancel}>
          Cancel
        </Button>,
      ]}>
      <p>
        Please confirm that you want to cancel <strong>all ongoing builds</strong> on
        change <strong>{refId}</strong> for project <strong>{projectName}</strong>.
      </p>
    </Modal>
  )
}

DequeueModal.propTypes = {
  item: PropTypes.object.isRequired,
  isOpen: PropTypes.bool.isRequired,
  onConfirm: PropTypes.func.isRequired,
  onCancel: PropTypes.func.isRequired,
}


function PromoteModal({ item, isOpen, onConfirm, onCancel }) {
  const ref = getRefs(item)[0]
  let refId = ref.id || 'N/A'
  return (
    <Modal
      variant={ModalVariant.small}
      isOpen={isOpen}
      title="You are about to promote a change"
      onClose={onCancel}
      actions={[
        <Button key="prom_confirm" variant="primary" onClick={onConfirm}>
          Confirm
        </Button>,
        <Button key="prom_cancel" variant="link" onClick={onCancel}>
          Cancel
        </Button>,
      ]}
    >
      <p>Please confirm that you want to promote a change <strong>{refId}</strong>.</p>
    </Modal>
  )
}

PromoteModal.propTypes = {
  item: PropTypes.object.isRequired,
  isOpen: PropTypes.bool.isRequired,
  onConfirm: PropTypes.func.isRequired,
  onCancel: PropTypes.func.isRequired,
}

function QueueItem({ item, pipeline, tenant, user, jobsExpanded }) {
  /* pipeline is allowed to be null only to support the legacy
   * ChangeStatus page (at status/change/...) where we don't know the
   * pipeline of the changes returned by the api.  Whatever replaces
   * that page should include pipeline information so that we regain
   * full functionality here.
   */
  const [isAdminActionsOpen, setIsAdminActionsOpen] = useState(false)
  const [isDequeueModalOpen, setIsDequeueModalOpen] = useState(false)
  const [isPromoteModalOpen, setIsPromoteModalOpen] = useState(false)
  const [isSkippedJobsExpanded, setIsSkippedJobsExpanded] = useState(false)

  const expansionKey = item.id
  const expandedJobs = useSelector(state => state.statusExpansion.expandedJobs[expansionKey])

  const skippedjobs = item.jobs.filter(j => getJobStrResult(j) === 'skipped')
  const jobs = item.jobs.filter(j => getJobStrResult(j) !== 'skipped')
  const dispatch = useDispatch()
  const isJobsExpanded = expandedJobs === undefined ? jobsExpanded : expandedJobs

  const onJobsToggle = (isExpanded) => {
    if (isExpanded) {
      dispatch(expandJobs(expansionKey))
    } else {
      dispatch(collapseJobs(expansionKey))
    }
  }

  const onSkippedJobsToggle = () => {
    setIsSkippedJobsExpanded(!isSkippedJobsExpanded)
  }

  const onSelect = () => {
    setIsAdminActionsOpen(!isAdminActionsOpen)
  }

  const showDequeueModal = () => {
    setIsDequeueModalOpen(true)
  }

  const showPromoteModal = () => {
    setIsPromoteModalOpen(true)
  }

  const confirmDequeue = () => {
    const ref = getRefs(item)[0]
    // Use the first ref as a proxy for the item since queue
    // commands operate on changes
    let projectName = ref.project
    let refId = ref.id || 'N/A'
    let refRef = ref.ref

    // close the modal
    setIsDequeueModalOpen(false)

    if (/^[0-9a-f]{40}$/.test(refId)) {
      // post-merge with a ref update (tag, branch push)
      dequeue_ref(tenant.apiPrefix, projectName, pipeline.name, refRef)
        .then(() => {
          dispatch(fetchStatusIfNeeded(tenant))
        })
        .catch(error => {
          dispatch(addDequeueError(error))
        })
    } else if (refId !== 'N/A') {
      // pre-merge, ie we have a change id
      dequeue(tenant.apiPrefix, projectName, pipeline.name, refId)
        .then(() => {
          dispatch(fetchStatusIfNeeded(tenant))
        })
        .catch(error => {
          dispatch(addDequeueError(error))
        })
    } else {
      // periodic with only a ref (branch head)
      dequeue_ref(tenant.apiPrefix, projectName, pipeline.name, refRef)
        .then(() => {
          dispatch(fetchStatusIfNeeded(tenant))
        })
        .catch(error => {
          dispatch(addDequeueError(error))
        })
    }
  }

  const confirmPromote = () => {
    const ref = getRefs(item)[0]
    let refId = ref.id || 'NA'

    // close the modal
    setIsPromoteModalOpen(false)

    if (refId !== 'N/A') {
      promote(tenant.apiPrefix, pipeline.name, [refId,])
        .then(() => {
          dispatch(fetchStatusIfNeeded(tenant))
        })
        .catch(error => {
          dispatch(addPromoteError(error))
        })
    } else {
      dispatch(addNotification({
        url: null,
        status: 'Invalid change ' + refId + ' for promotion',
        text: '',
        type: 'error'
      }))
    }
  }

  const cancelDequeue = () => {
    setIsDequeueModalOpen(false)
  }

  const cancelPromote = () => {
    setIsPromoteModalOpen(false)
  }

  const times = calculateQueueItemTimes(item)

  const adminActions = []

  if (isAuthorized(user, 'dequeue')) {
    adminActions.push(
      <DropdownItem
        key="dequeue"
        icon={<BanIcon style={{
          color: 'var(--pf-global--danger-color--100)',
        }} />}
        description="Stop all jobs for this change"
        onClick={() => showDequeueModal()}
      >
        Dequeue
      </DropdownItem>
    )
  }
  if (isAuthorized(user, 'promote')) {
    adminActions.push(
      <DropdownItem
        key="promote"
        icon={<AngleDoubleUpIcon style={{
          color: 'var(--pf-global--default-color--200)',
        }} />}
        description="Promote this change to the top of the queue"
        onClick={() => showPromoteModal()}
      >
        Promote
      </DropdownItem>
    )
  }

  return (
    <>
      <Card isCompact className={`zuul-compact-card ${item.live === true ? 'zuul-queue-item' : ''}`}>
        <CardHeader>
          {item.live === true ?
            <CardActions>
              {pipeline && <FilterDropdown item={item} pipeline={pipeline} />}
              {pipeline && (adminActions.length > 0) ?
                <Dropdown
                  className="zuul-admin-dropdown"
                  onSelect={onSelect}
                  toggle={<KebabToggle onToggle={setIsAdminActionsOpen} />}
                  isOpen={isAdminActionsOpen}
                  isPlain
                  dropdownItems={adminActions}
                  position="right"
                /> : ''}
            </CardActions>
            : ''}
          <CardTitle>
            {getRefs(item).map((change, idx) => (
              <div key={idx}>
                {change.project} <ChangeLink change={change} />
              </div>
            ))}
          </CardTitle>
        </CardHeader>
        {item.live === true ?
          <CardBody>
            <QueueItemProgress item={item} times={times} />
            {item.jobs.length > 0 ?
              <ExpandableSection className="zuul-compact-expendable-section"
                toggleContent={isJobsExpanded ? 'Hide jobs' : 'Show jobs'}
                onToggle={onJobsToggle}
                isExpanded={isJobsExpanded}
              >
                <JobList
                  jobs={jobs}
                  skippedJobs={skippedjobs}
                  isSkippedJobsExpanded={isSkippedJobsExpanded}
                  onSkippedJobsToggle={onSkippedJobsToggle}
                  tenant={tenant}
                  job_times={times.jobs}
                />
              </ExpandableSection >
              : ''
            }
          </CardBody >
          : ''}
      </Card >
      <DequeueModal
        item={item}
        isOpen={isDequeueModalOpen}
        onConfirm={confirmDequeue}
        onCancel={cancelDequeue}
      />
      <PromoteModal
        item={item}
        isOpen={isPromoteModalOpen}
        onConfirm={confirmPromote}
        onCancel={cancelPromote}
      />
    </>
  )
}

QueueItem.propTypes = {
  item: PropTypes.object,
  pipeline: PropTypes.object,
  tenant: PropTypes.object,
  user: PropTypes.object,
  jobsExpanded: PropTypes.bool,
}

export default connect(state => ({
  tenant: state.tenant,
  user: state.user,
}))(QueueItem)
