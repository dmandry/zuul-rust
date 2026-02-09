// Copyright 2020 Red Hat, Inc
// Copyright 2022, 2024 Acme Gating, LLC
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
import { useSelector, useDispatch } from 'react-redux'
import PropTypes from 'prop-types'
import {
  Button,
  EmptyState,
  EmptyStateBody,
  Spinner,
  Title,
  Modal,
  ModalVariant,
} from '@patternfly/react-core'
import {
  Table,
  TableHeader,
  TableBody,
  TableVariant,
  expandable,
} from '@patternfly/react-table'
import { Link } from 'react-router-dom'
import ImageUploadTable from './ImageUploadTable'
import { fetchImages } from '../../actions/images'
import { fetchProviders } from '../../actions/providers'
import { addNotification, addApiError } from '../../actions/notifications'
import { deleteImageBuildArtifact } from '../../api'
import { isAuthorized } from '../../Misc'

const STATE_STYLES = {
  ready: {
    color: 'var(--pf-global--success-color--100)',
  },
  deleting: {
    color: 'var(--pf-global--info-color--100)',
  },
}

function ImageBuildTable(props) {
  const { buildArtifacts, fetching } = props
  const [collapsedRows, setCollapsedRows] = useState([])
  const [showDeleteImageModal, setShowDeleteImageModal] = useState(false)
  const [pendingDeleteRow, setPendingDeleteRow] = useState(null)
  const setRowCollapsed = (idx, isCollapsing = true) =>
        setCollapsedRows(prevCollapsed => {
          const otherCollapsedRows = prevCollapsed.filter(r => r !== idx)
          return isCollapsing ?
                [...otherCollapsedRows, idx] : otherCollapsedRows
        })
  const isRowCollapsed = idx => collapsedRows.includes(idx)
  const tenant = useSelector((state) => state.tenant)
  const user = useSelector((state) => state.user)
  const dispatch = useDispatch()

  const columns = [
    {
      title: 'UUID',
      dataLabel: 'UUID',
      cellFormatters: [expandable],
    },
    {
      title: 'Timestamp',
      dataLabel: 'Timestamp',
    },
    {
      title: 'Validated',
      dataLabel: 'Validated',
    },
    {
      title: 'State',
      dataLabel: 'State',
    },
    {
      title: 'State Time',
      dataLabel: 'State Time',
    },
    {
      title: 'Locked',
      dataLabel: 'Locked',
    },
    {
      title: 'Build',
      dataLabel: 'Build',
    },
  ]

  function createImageBuildRow(rows, build) {
    // Only link to the build if it is in this tenant
    const buildUUID = build.build_tenant?
          <Link to={`${tenant.linkPrefix}/build/${build.build_uuid}`}>
            {build.build_uuid}
          </Link>
          :
          build.build_uuid
    const state_style = STATE_STYLES[build.state] || {}
    return {
      _uuid: build.uuid,
      id: rows.length,
      isOpen: !isRowCollapsed(rows.length),
      canDelete: build.build_tenant,
      cells: [
        {
          title: build.uuid
        },
        {
          title: build.timestamp
        },
        {
          title: build.validated ? 'validated' : 'unvalidated'
        },
        {
          title: <span style={state_style}>{build.state}</span>
        },
        {
          title: build.state_time
        },
        {
          title: build.locke_holder
        },
        {
          title: buildUUID
        },
      ]
    }
  }

  function createImageUploadRow(rows, parent, build) {
    return {
      id: rows.length,
      parent: parent.id,
      cells: [
        {
          title: <ImageUploadTable
                   build={build}
                   uploads={build.uploads}
                   fetching={fetching}/>
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

  const haveBuildArtifacts = buildArtifacts && buildArtifacts.length > 0

  let rows = []
  if (fetching) {
    rows = createFetchingRow()
    columns[0].dataLabel = ''
  } else {
    if (haveBuildArtifacts) {
      rows = []
      buildArtifacts.forEach(build => {
        let buildRow = createImageBuildRow(rows, build)
        rows.push(buildRow)
        rows.push(createImageUploadRow(rows, buildRow, build))
      })
    }
  }

  const actionResolver = (rowData) => {
    if (rowData.parent === undefined &&
        rowData.canDelete &&
        isAuthorized(user, 'delete-image-build-artifact')) {
      return [
        {
          title: 'Delete build',
          onClick: () => {
            setPendingDeleteRow(rowData)
            setShowDeleteImageModal(true)
          }
        },
      ]
    }
    return []
  }

  function renderDeleteImageModal() {
    const title = 'Delete image build'
    return (
      <Modal
        variant={ModalVariant.small}
        isOpen={showDeleteImageModal}
        title={title}
        onClose={() => { setShowDeleteImageModal(false) }}
        actions={[
          <Button key="confirm" variant="primary"
                  onClick={() => {
                    setShowDeleteImageModal(false)
                    deleteImageBuildArtifact(tenant.apiPrefix,
                                             pendingDeleteRow._uuid
                                            ).then(() => {
                      dispatch(addNotification(
                        {
                          text: 'Delete successful.',
                          type: 'success',
                          status: '',
                          url: '',
                        }))
                      dispatch(fetchProviders(tenant))
                      dispatch(fetchImages(tenant))
                    })
                      .catch(error => {
                        dispatch(addApiError(error))
                      })
                  }}>
            Confirm
          </Button>,
          <Button key="cancel" variant="link"
                  onClick={() => {setShowDeleteImageModal(false) }}>
            Cancel
          </Button>,
        ]}>
        <p>
          Please confirm that you want to delete this image
          and all of its uploads.
        </p>
      </Modal>
    )
  }

  return (
    <>
      <Title headingLevel="h3">
        Image Build Artifacts
      </Title>
      <Table
        aria-label="Image Build Table"
        variant={TableVariant.compact}
        cells={columns}
        rows={rows}
        actionResolver={actionResolver}
        onCollapse={(_event, rowIndex, isOpen) => {
          setRowCollapsed(rowIndex, !isOpen)
        }}
      >
        <TableHeader />
        <TableBody />
      </Table>

      {/* Show an empty state in case we don't have any build artifacts but are also not
          fetching */}
      {!fetching && !haveBuildArtifacts && (
        <EmptyState>
          <Title headingLevel="h1">No build artifacts found</Title>
          <EmptyStateBody>
            Nothing to display.
          </EmptyStateBody>
        </EmptyState>
      )}
      {renderDeleteImageModal()}
    </>
  )
}

ImageBuildTable.propTypes = {
  buildArtifacts: PropTypes.array,
  fetching: PropTypes.bool.isRequired,
}

export default ImageBuildTable
