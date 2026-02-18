// Copyright 2020 Red Hat, Inc
// Copyright 2022 Acme Gating, LLC
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

import React, { useEffect } from 'react'
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import { useHistory } from 'react-router-dom'
import {
  EmptyState,
  EmptyStateBody,
  EmptyStateIcon,
  Spinner,
  Title,
} from '@patternfly/react-core'
import {
  FingerprintIcon,
} from '@patternfly/react-icons'

// Several pages use the location hash in a way that would be
// difficult to disentangle from the OIDC callback parameters.  This
// dedicated callback page accepts the OIDC params and then internally
// redirects to the page we saved before redirecting to the IDP.

function AuthCallbackPage(props) {
  const history = useHistory()
  const { user } = props

  useEffect(() => {
    if (user.redirect) {
      history.push(user.redirect)
    } else if (user.data) {
      // If the page is currently on AuthCallback, but the user state got updated
      // without redirect, it is updated by receiving a token from another tab.
      // This could happen in race condition in some Browser (e.g. Chrome), that
      // the login process is triggered before a token is received from another tab.
      // In this case, we fall back to the redirect target saved in localStorage,
      // other wise it would hanging on the AuthCallback page.
      history.push(localStorage.getItem('zuul_auth_redirect'))
    }
  }, [history, user])

  return (
    <>
        <EmptyState>
          <EmptyStateIcon icon={FingerprintIcon} />
          <Title headingLevel="h1">Login in progress</Title>
          <EmptyStateBody>
            <p>
              You will be redirected shortly...
            </p>
            <Spinner size="xl" />
          </EmptyStateBody>
        </EmptyState>
    </>
  )
}

AuthCallbackPage.propTypes = {
    user: PropTypes.object,
}

export default connect((state) => ({
  user: state.user,
}))(AuthCallbackPage)
