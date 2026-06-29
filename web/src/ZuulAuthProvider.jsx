// Copyright 2020 Red Hat, Inc
// Copyright 2021 Acme Gating, LLC
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

import { AuthProvider } from 'oidc-react'
import { userLoggedIn, userLoggedOut } from './actions/user'
import { UserManager, User } from 'oidc-client'
import { getHomepageUrl } from './api'
import _ from 'lodash'


class ZuulAuthProvider extends React.Component {
  /*
    This wraps the oidc-react AuthProvider and supplies the necessary
    information as props.

    The oidc-react AuthProvider is not really meant to be reconstructed
    frequently. Calling render multiple times (even if nothing actually
    changes) during a login can cause multiple AuthProviders to be created
    which can interfere with the login process.

    We connect this class to state.auth.auth_params, so make sure that isn't
    updated unless the OIDC parameters are actually changed.

    If they are changed, then we will create a new AuthProvider with the
    new parameters.  Save those parameters in local storage so that when
    we return from the IDP redirect, an AuthProvider with the same
    configuration is created.
   */
  static propTypes = {
    auth_params: PropTypes.object,
    channel: PropTypes.object,
    election: PropTypes.object,
    dispatch: PropTypes.func,
    children: PropTypes.any,
  }

  render() {
    const { auth_params, channel, election } = this.props

    console.debug('ZuulAuthProvider rendering with params', auth_params)

    const userManager = new UserManager({
      ...auth_params,
      response_type: 'token id_token',
      silent_redirect_uri: getHomepageUrl() + 'silent_callback',
      redirect_uri: getHomepageUrl() + 'auth_callback',
      includeIdTokenInSilentRenew: false,
    })

    const oidcConfig = {
      onSignIn: async (user) => {
        // Update redux with the logged in state and send the
        // credentials to any other tabs.
        const redirect = sessionStorage.getItem('zuul_auth_redirect') ||
              localStorage.getItem('zuul_auth_redirect')
        this.props.dispatch(userLoggedIn(user, redirect))
        this.props.channel.postMessage({
          type: 'signIn',
          auth_params: auth_params,
          user: user
        })
      },
      onSignOut: async () => {
        // Update redux with the logged out state and send the
        // credentials to any other tabs.
        this.props.dispatch(userLoggedOut())
        this.props.channel.postMessage({
          type: 'signOut',
          auth_params: auth_params
        })
      },
      autoSignIn: false,
      userManager: userManager,
    }

    // This is called whenever we receive a message from another tab
    channel.onmessage = (msg) => {
      console.debug('Received broadcast message', msg)

      if (msg.type === 'signIn' && _.isEqual(msg.auth_params, auth_params)) {
        const user = new User(msg.user)
        userManager.getUser().then((olduser) => {
          // In some cases, we can receive our own message, so make
          // sure that the user info we received is different from
          // what we already have.
          let needToUpdate = true
          if (olduser) {
            if (user.toStorageString() === olduser.toStorageString()) {
              needToUpdate = false
            }
          }
          if (needToUpdate) {
            console.debug('New token from other tab')
            userManager.storeUser(user)
            userManager.events.load(user)
            this.props.dispatch(userLoggedIn(user))
          }
        })
      }
      else if (msg.type === 'signOut' && _.isEqual(msg.auth_params, auth_params)) {
        userManager.removeUser()
        this.props.dispatch(userLoggedOut())
      }
      else if (msg.type === 'init') {
        // A new tab has been created; send our token in case it's helpful.
        userManager.getUser().then((user) => {
          if (user) {
            console.debug('Sending token to new tab')
            this.props.channel.postMessage({
              type: 'signIn',
              auth_params: auth_params,
              user: user
            })
          }
        })
      }
    }

    // If we already have user data saved in session storage, we need to
    // tell redux about it.
    userManager.getUser().then((user) => {
      if (user) {
        console.debug('Restoring initial login from userManager')
        this.props.dispatch(userLoggedIn(user))
      } else {
        // Maybe another tab is logged in.  Ask them to send us tokens.
        console.debug('Asking other tabs for auth tokens')
        this.props.channel.postMessage({ type: 'init' })
      }
    })

    // This is called about 1 minute before a token is expired.  We will try
    // to renew the token.  We use a leader election so that only one tab
    // makes the attempt; the others will receive the token via a broadcast
    // event.
    userManager.events.addAccessTokenExpiring(() => {
      if (election.isLeader) {
        console.debug('Token is expiring; renewing')
        userManager.signinSilent().then(user => {
          console.debug('Token renewal successful')
          this.props.dispatch(userLoggedIn(user))
          channel.postMessage({
            type: 'signIn',
            auth_params: auth_params,
            user: user
          })
        }, err => {
          console.error('Error renewing token:', err.message)
        })
      } else {
        console.debug('Token is expiring; expecting leader to renew')
      }
    })

    return (
      <React.Fragment>
        <AuthProvider {...oidcConfig} key={JSON.stringify(auth_params)}>
          {this.props.children}
        </AuthProvider>
      </React.Fragment>
    )
  }
}

export default connect(state => ({
  auth_params: state.auth.auth_params,
}))(ZuulAuthProvider)
