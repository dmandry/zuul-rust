// Copyright 2020 Red Hat, Inc
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

import * as React from 'react'
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import { Flex, FlexItem, PageSection, PageSectionVariants } from '@patternfly/react-core'

import { fetchAutoholdsIfNeeded } from '../actions/autoholds'
import AutoholdTable from '../containers/autohold/AutoholdTable'

import {
  AddCircleOIcon,
} from '@patternfly/react-icons'
import AutoholdModal from '../containers/autohold/autoholdModal'
import { IconProperty } from '../MiscComponents'
import { isAuthorized } from '../Misc'


class AutoholdsPage extends React.Component {
  static propTypes = {
    tenant: PropTypes.object,
    user: PropTypes.object,
    remoteData: PropTypes.object,
    dispatch: PropTypes.func
  }

  constructor() {
    super()

    this.state = {
      showAutoholdModal: false,
    }
  }

  setShowAutoholdModal = (value) => {
    this.setState(() => ({ showAutoholdModal: value }))
  }

  renderAutoholdButton() {
    const value = (<span style={{
      cursor: 'pointer',
      color: 'var(--pf-global--primary-color--100)'
    }}
      title="Create a new autohold request to hold nodes in case of a build failure"
      onClick={(event) => {
        event.preventDefault()
        this.setShowAutoholdModal(true)
      }}
    >
      Create Request
    </span>)
    return (
      <IconProperty
        icon={<AddCircleOIcon />}
        value={value}
      />

    )
  }

  updateData = (force) => {
    this.props.dispatch(fetchAutoholdsIfNeeded(this.props.tenant, force))
  }

  componentDidMount() {
    document.title = 'Zuul Autoholds'
    if (this.props.tenant.name) {
      this.updateData()
    }
  }

  componentDidUpdate(prevProps) {
    if (this.props.tenant.name !== prevProps.tenant.name) {
      this.updateData()
    }
  }

  render() {
    const { user, remoteData } = this.props
    const autoholds = remoteData.autoholds
    const { showAutoholdModal } = this.state

    return (
      <>
        {isAuthorized(user, 'autohold') && (
          <PageSection>
            <Flex flex={{ default: 'flex_1' }}>
              <FlexItem>
                {this.renderAutoholdButton()}
              </FlexItem>
            </Flex>
          </PageSection>
        )}
        <PageSection variant={PageSectionVariants.light}>
          <AutoholdTable
            autoholds={autoholds}
            fetching={remoteData.isFetching} />
        </PageSection>
        {<AutoholdModal
          showAutoholdModal={showAutoholdModal}
          setShowAutoholdModal={this.setShowAutoholdModal}
        />}
      </>
    )
  }
}

export default connect(state => ({
  tenant: state.tenant,
  remoteData: state.autoholds,
  user: state.user,
}))(AutoholdsPage)
