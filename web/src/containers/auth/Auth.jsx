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

import React from 'react'
import PropTypes from 'prop-types'
import { connect } from 'react-redux'

import {
  Accordion,
  AccordionItem,
  AccordionToggle,
  AccordionContent,
  Button,
  ButtonVariant,
  ClipboardCopy,
  ClipboardCopyVariant,
  List,
  ListItem,
  Modal,
  ModalVariant
} from '@patternfly/react-core'
import {
  UserIcon,
  SignInAltIcon,
  SignOutAltIcon,
  HatWizardIcon
} from '@patternfly/react-icons'

import * as moment_tz from 'moment-timezone'

import { apiUrl } from '../../api'
import { fetchUserACL } from '../../actions/user'
import { withAuth } from 'oidc-react'
import { getHomepageUrl } from '../../api'


class AuthContainerComponent extends React.Component {
  static propTypes = {
    user: PropTypes.object,
    tenant: PropTypes.object,
    dispatch: PropTypes.func.isRequired,
    timezone: PropTypes.string.isRequired,
    info: PropTypes.object,
    auth: PropTypes.object,
    // Props coming from withAuth
    userManager: PropTypes.object,
    signIn: PropTypes.func,
    signOut: PropTypes.func,
  }

  constructor(props) {
    super(props)
    this.state = {
      isModalOpen: false,
      showZuulClientConfig: false,
      isSessionExpiredModalOpen: false,
    }
    this.handleModalToggle = () => {
      this.setState(({ isModalOpen }) => ({
        isModalOpen: !isModalOpen
      }))
    }
    this.handleSessionExpiredModalToggle = () => {
      this.setState(({ isSessionExpiredModalOpen }) => ({
        isSessionExpiredModalOpen: !isSessionExpiredModalOpen
      }))
    }
    this.handleConfigToggle = () => {
      this.setState(({ showZuulClientConfig }) => ({
        showZuulClientConfig: !showZuulClientConfig
      }))
    }
    this.onAccessTokenExpired = () => {
      // If the token has expired, show the modal
      console.debug('Token expired')
      this.setState(() => ({
        isSessionExpiredModalOpen: true,
        isModalOpen: false,
      }))
    }
    this.onUserLoaded = () => {
      // If another tab logged in while our expired modal is shown, go
      // ahead and clear it.
      console.debug('User signed in')
      this.setState(() => ({
        isSessionExpiredModalOpen: false,
      }))
    }
  }

  clickOnSignIn() {
    const redirect_target = window.location.href.slice(getHomepageUrl().length)
    localStorage.setItem('zuul_auth_redirect', redirect_target)
    this.props.signIn()
  }

  componentDidMount() {
    const { user, tenant } = this.props
    this.props.userManager.events.addAccessTokenExpired(this.onAccessTokenExpired)
    this.props.userManager.events.addUserLoaded(this.onUserLoaded)

    if (user.data) {
      console.log('Refreshing ACL', user.tenant, tenant)
      this.props.dispatch(fetchUserACL(tenant))
    }
  }

  componentWillUnmount() {
    this.props.userManager.events.removeAccessTokenExpired(this.onAccessTokenExpired)
    this.props.userManager.events.removeUserLoaded(this.onUserLoaded)
  }

  componentDidUpdate() {
    const { user, tenant } = this.props

    // Make sure the token is current and the tenant is up to date.
    if (user.data && user.tenant !== tenant.name) {
      console.log('Refreshing ACL', user.tenant, tenant)
      this.props.dispatch(fetchUserACL(tenant))
    }
  }

  ZuulClientConfig() {
    const { user, tenant } = this.props

    let ZCconfig
    ZCconfig = '[' + tenant.name + ']\n'
    ZCconfig = ZCconfig + 'url=' + apiUrl.slice(0, -4) + '\n'
    ZCconfig = ZCconfig + 'tenant=' + tenant.name + '\n'
    ZCconfig = ZCconfig + 'auth_token=' + user.token + '\n'

    return ZCconfig
  }

  renderCredentialsExpiredModal() {
    const { isSessionExpiredModalOpen } = this.state
    return <React.Fragment>
      <Modal
        position='top'
        title='You are being logged out'
        isOpen={isSessionExpiredModalOpen}
        variant={ModalVariant.small}
        onClose={() => {
          this.handleSessionExpiredModalToggle()
          this.props.signOut()
        }}>
        Your session has expired. &nbsp;
        <Button
          key="SignIn"
          isInline
          variant={ButtonVariant.link}
          onClick={() => { this.clickOnSignIn() }}>
          Please renew your credentials.
        </Button>
      </Modal>
    </React.Fragment>
  }

  renderModal() {
    const { user, tenant, timezone } = this.props
    const { isModalOpen, showZuulClientConfig } = this.state
    let config = this.ZuulClientConfig(tenant, user.data)
    let valid_until = moment_tz.unix(user.data.expires_at).tz(timezone).format('YYYY-MM-DD HH:mm:ss')
    console.log("JEB", user.permissions)
    return (
      <React.Fragment>
        <Modal
          variant={ModalVariant.small}
          title="User Info"
          isOpen={isModalOpen}
          onClose={this.handleModalToggle}
          actions={[
            <Button
              key="SignOut"
              variant="primary"
              onClick={() => {
                this.props.signOut()
              }}
              title="Note that you will be logged out of Zuul, but not out of your identity provider.">
              Sign Out &nbsp;
              <SignOutAltIcon title='Sign Out' />
            </Button>
          ]}
        >
          <div>
            <p key="user">Name: <strong>{user.data.profile.name}</strong></p>
            <p key="preferred_username">Logged in as: <strong>{user.data.profile.preferred_username}</strong></p>
            <p>Permissions:</p>
            <List>
              {
                Object.entries(user.permissions).map((perm, value) =>
                  <ListItem>{perm}</ListItem>)
              }
            </List>
            <Accordion asDefinitionList>
              <AccordionItem>
                <AccordionToggle
                  onClick={this.handleConfigToggle}
                  isExpanded={showZuulClientConfig}
                  title='Configuration parameters that can be used to perform tasks with the CLI'
                  id="ZCConfig">
                  Show Zuul Client Config
                </AccordionToggle>
                <AccordionContent
                  isHidden={!showZuulClientConfig}>
                  <ClipboardCopy isCode isReadOnly variant={ClipboardCopyVariant.expansion}>{config}</ClipboardCopy>
                </AccordionContent>
              </AccordionItem>
            </Accordion>
            <p key="valid_until">Token expiry date: <strong>{valid_until}</strong></p>
            <p key="footer">
              Zuul stores and uses information such as your username
              and your email to provide some features. This data is
              stored <strong>in your browser only</strong> and is
              discarded once you log out.
            </p>
          </div>
        </Modal>
      </React.Fragment>
    )
  }

  renderButton(containerStyles) {

    const { user } = this.props
    if (!user.data) {
      return (
        <div style={containerStyles}>
          <Button
            key="SignIn"
            variant={ButtonVariant.plain}
            onClick={() => { this.clickOnSignIn() }}>
            Sign in &nbsp;
            <SignInAltIcon title='Sign In' />
          </Button>
        </div>
      )
    } else {
      return (user.data.isFetching ? <div style={containerStyles}>Loading...</div> :
        <div style={containerStyles}>
          {this.renderModal()}
          <Button
            variant={ButtonVariant.plain}
            key="userinfo"
            onClick={this.handleModalToggle}>
            <UserIcon title='User details' />
            &nbsp;{user.data.profile.preferred_username}&nbsp;
          </Button>
          {this.renderCredentialsExpiredModal()}
        </div>
      )
    }
  }

  render() {
    const { info, auth } = this.props
    const textColor = '#d1d1d1'
    const containerStyles = {
      color: textColor,
      border: 'solid #2b2b2b',
      borderWidth: '0 0 0 1px',
      display: 'initial',
      padding: '6px'
    }

    if (info.isFetching) {
      return (<><div style={containerStyles}>Fetching auth info ...</div></>)
    }
    // auth_params.authority is only set if an OpenID Connect auth is available
    if (auth.info && auth.info.default_realm && auth.auth_params.authority) {
      return this.renderButton(containerStyles)
    } else {
      return (<div style={containerStyles} title='Authentication disabled'>-</div>)
    }
  }
}

const AuthContainer = connect(state => ({
  auth: state.auth,
  user: state.user,
  tenant: state.tenant,
  timezone: state.timezone,
  info: state.info,
}))(withAuth(AuthContainerComponent))

export default AuthContainer
