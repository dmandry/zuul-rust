// Copyright 2024 Acme Gating, LLC
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
import {
  Button,
  DescriptionList,
  DescriptionListTerm,
  DescriptionListGroup,
  DescriptionListDescription,
  Modal,
  ModalVariant,
} from '@patternfly/react-core'
import PropTypes from 'prop-types'
import { addNotification, addApiError } from '../../actions/notifications'
import { buildImage } from '../../api'
import { isAuthorized } from '../../Misc'

function ProviderDetail(props) {
  const {image} = props
  const [showBuildModal, setShowBuildModal] = useState(false)
  const tenant = useSelector((state) => state.tenant)
  const user = useSelector((state) => state.user)
  const dispatch = useDispatch()

  function renderBuildModal() {
    return (
      <Modal
        variant={ModalVariant.small}
        isOpen={showBuildModal}
        title="Start image build"
        onClose={() => { setShowBuildModal(false) }}
        actions={[
          <Button key="confirm" variant="primary"
                  onClick={() => {
                    setShowBuildModal(false)
                    buildImage(tenant.apiPrefix,
                               image.name
                              ).then(() => {
                      dispatch(addNotification(
                        {
                          text: 'Image build triggered.',
                          type: 'success',
                          status: '',
                          url: '',
                        }))
                    })
                      .catch(error => {
                        dispatch(addApiError(error))
                      })
                  }}>
            Confirm
          </Button>,
          <Button key="cancel" variant="link"
                  onClick={() => { setShowBuildModal(false) }}>
            Cancel</Button>,
        ]}>
        <p>Please confirm that you want to trigger a build of this image.</p>
      </Modal>
    )
  }

  return (
    <>
      <DescriptionList isHorizontal
                       style={{'--pf-c-description-list--RowGap': '0rem'}}
                       className='pf-u-m-xl'>
        <DescriptionListGroup>
          <DescriptionListTerm>
            Name
          </DescriptionListTerm>
          <DescriptionListDescription>
            {image.name}
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>
            Canonical Name
          </DescriptionListTerm>
          <DescriptionListDescription>
            {image.canonical_name}
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>
            Type
          </DescriptionListTerm>
          <DescriptionListDescription>
            {image.type}
          </DescriptionListDescription>
        </DescriptionListGroup>
      </DescriptionList>

      {isAuthorized(user, 'build-image') &&
       <Button onClick={() => {setShowBuildModal(true)}}>
         Build Image
       </Button>
      }
      {renderBuildModal()}
    </>
  )
}

ProviderDetail.propTypes = {
  image: PropTypes.object.isRequired,
}

export default ProviderDetail
