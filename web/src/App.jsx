// Copyright 2018 Red Hat, Inc
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

// The App is the parent component of every pages. Each page content is
// rendered by the Route object according to the current location.

import React from 'react'
import PropTypes from 'prop-types'
import { matchPath, withRouter } from 'react-router'
import { Link, NavLink, Redirect, Route, Switch } from 'react-router-dom'
import { connect } from 'react-redux'
import { withAuth } from 'oidc-react'
import * as moment_tz from 'moment-timezone'
import {
  Alert,
  AlertActionCloseButton,
  AlertGroup,
  Brand,
  Button,
  ButtonVariant,
  Dropdown,
  DropdownItem,
  DropdownToggle,
  DropdownSeparator,
  KebabToggle,
  Nav,
  NavItem,
  NavList,
  NotificationBadge,
  Page,
  PageHeader,
  PageHeaderTools,
  PageHeaderToolsGroup,
  PageHeaderToolsItem,
} from '@patternfly/react-core'

import {
  BellIcon,
  BookIcon,
  ChevronDownIcon,
  CodeIcon,
  ServiceIcon,
  UsersIcon,
} from '@patternfly/react-icons'

import AuthContainer from './containers/auth/Auth'
import ErrorBoundary from './containers/ErrorBoundary'
import { Fetching } from './containers/Fetching'
import SelectTz from './containers/timezone/SelectTz'
import ConfigModal from './containers/config/Config'
import logo from './images/logo.svg'
import { clearNotification } from './actions/notifications'
import { fetchTenantStatusAction, clearTenantStatusAction } from './actions/tenantStatus'
import { fetchTenantsIfNeeded } from './actions/tenants'
import { routes } from './routes'
import { setTenantAction } from './actions/tenant'
import { configureAuthFromTenant, configureAuthFromInfo } from './actions/auth'
import { userBroadcastWaitStart, userBroadcastWaitFinish } from './actions/user'
import { getHomepageUrl } from './api'
import AuthCallbackPage from './pages/AuthCallback'
import AuthRequiredPage from './pages/AuthRequired'

class AppComponent extends React.Component {
  static propTypes = {
    notifications: PropTypes.array,
    tenantStatus: PropTypes.object,
    tenantStatusReady: PropTypes.bool,
    info: PropTypes.object,
    tenant: PropTypes.object,
    tenants: PropTypes.object,
    timezone: PropTypes.string,
    location: PropTypes.object,
    history: PropTypes.object,
    dispatch: PropTypes.func,
    isKebabDropdownOpen: PropTypes.bool,
    user: PropTypes.object,
    auth: PropTypes.object,
    signIn: PropTypes.func,
  }

  state = {
    isTenantDropdownOpen: false,
  }

  renderMenu(menu) {
    const { tenant } = this.props
    if (tenant.name) {
      return (
        <Nav aria-label="Nav" variant="horizontal">
          <NavList>
            {menu.filter(item => item.title).map(item => (
              <NavItem itemId={item.to} key={item.to}>
                <NavLink
                  to={tenant.linkPrefix + item.to}
                  activeClassName="pf-c-nav__link pf-m-current"
                >
                  {item.title}
                </NavLink>
              </NavItem>
            ))}
          </NavList>
        </Nav>
      )
    } else {
      // Return an empty navigation bar in case we don't have an active tenant
      return <Nav aria-label="Nav" variant="horizontal" />
    }
  }

  isAuthReady() {
    const { info, auth, user } = this.props
    return !(info.isFetching ||
             !auth.info ||
             auth.isFetching ||
             user.isFetching ||
             (auth.info.read_protected && !user.data && !user.broadcastWaitFinish)
            )
  }

  renderContent = (menu) => {
    const { tenant, auth, user } = this.props
    const allRoutes = []

    if ((window.location.origin + window.location.pathname) ===
        (getHomepageUrl() + 'auth_callback')) {
      // Sit on the auth callback page until login and token
      // validation is complete (it will internally redirect when complete)
      return <AuthCallbackPage/>
    }
    if (!this.isAuthReady()) {
      return <Fetching />
    }
    if (auth.info.read_protected && !user.data && !user.broadcastWaitFinish) {
      return <Fetching />
    }
    if (auth.info.read_protected && !user.data) {
      const redirect_target = window.location.href.slice(getHomepageUrl().length)
      // If the redirect_target is the root url, we set the zuul_auth_redirect to /
      // so that the auth callback page can redirect to the / after login
      localStorage.setItem('zuul_auth_redirect', redirect_target==='' ? '/' : redirect_target)
      this.props.signIn()
      return <Fetching />
    }
    if (auth.info.read_protected && user.scope.length<1) {
      return <AuthRequiredPage/>
    }
    menu
      // Do not include '/tenants' route in white-label setup
      .filter(item =>
        (tenant.whiteLabel && !item.globalRoute) || !tenant.whiteLabel)
      .forEach((item, index) => {
        // We use react-router's render function to be able to pass custom props
        // to our route components (pages):
        // https://reactrouter.com/web/api/Route/render-func
        // https://learnwithparam.com/blog/how-to-pass-props-in-react-router/
        allRoutes.push(
          <Route
            key={index}
            path={
              item.globalRoute ? item.to :
                item.noTenantPrefix ? item.to : tenant.routePrefix + item.to}
            render={routerProps => (
              <item.component
                {...item.props}
                // TODO: remove routerProps argument after all pages use hooks.
                {...routerProps}
              />
            )}
            exact
          />
        )
      })
    if (tenant.defaultRoute)
      allRoutes.push(
        <Redirect from='*' to={tenant.defaultRoute} key='default-route' />
      )
    return (
      <Switch>
        {allRoutes}
      </Switch>
    )
  }

  componentDidUpdate() {
    // This method is called when info property is updated
    const { tenant, info, auth, user, tenantStatusReady } = this.props
    if (info.ready) {
      let tenantName = null
      let whiteLabel

      if (auth.info && auth.info.read_protected && !user.data && !user.broadcastWaitStart) {
        console.log('Read-access login required')
        this.props.dispatch(userBroadcastWaitStart());
        setTimeout(() => {
          this.props.dispatch(userBroadcastWaitFinish());
        }, 100)
      }

      if (info.tenant) {
        // White label
        whiteLabel = true
        tenantName = info.tenant
      } else if (!info.tenant) {
        // Multi tenant, look for tenant name in url
        whiteLabel = false
        // Fetch tenants only when auth is done or not required
        // Otherwise it would enter a loop when api-root auth is configured
        if (!info.capabilities.auth.read_protected || user.data) {
          this.props.dispatch(fetchTenantsIfNeeded())
        }

        const match = matchPath(
          this.props.location.pathname, { path: '/t/:tenant' })

        if (match) {
          tenantName = match.params.tenant
        }
      }
      // Set tenant only if it changed to prevent DidUpdate loop
      if (tenant.name !== tenantName) {
        const tenantAction = setTenantAction(tenantName, whiteLabel)
        this.props.dispatch(tenantAction)
        if (tenantName) {
          this.props.dispatch(clearTenantStatusAction())
        }
        if (whiteLabel || !tenantName) {
          // The app info endpoint was already a tenant info
          // endpoint, so auth info was already provided.
          this.props.dispatch(configureAuthFromInfo(info))
        } else {
          // Query the tenant info endpoint for auth info.
          this.props.dispatch(configureAuthFromTenant(tenantName))
        }
      }
      if (tenant && tenant.name && !tenantStatusReady && this.isAuthReady() &&
          (!auth.info.read_protected || user.data)) {
        // This will happen after the tenant action is complete, so we
        // can use the "old" tenant now.
        this.props.dispatch(fetchTenantStatusAction(tenant))
      }
    }
  }

  handleKebabDropdownToggle = (isKebabDropdownOpen) => {
    this.setState({
      isKebabDropdownOpen
    })
  }

  handleKebabDropdownSelect = () => {
    this.setState({
      isKebabDropdownOpen: !this.state.isKebabDropdownOpen
    })
  }

  handleComponentsLink = () => {
    const { history } = this.props
    history.push('/components')
  }

  handleApiLink = () => {
    const { history } = this.props
    history.push('/openapi')
  }

  handleDocumentationLink = () => {
    window.open('https://zuul-ci.org/docs', '_blank', 'noopener noreferrer')
  }

  handleTenantLink = () => {
    const { history, tenant } = this.props
    history.push(tenant.defaultRoute)
  }

  renderNotifications = (notifications) => {
    return (
      <AlertGroup isToast isLiveRegion>
        {notifications.map(notification => {
          let notificationBody
          if (notification.type === 'error') {
            notificationBody = (
              <>
                <strong>{notification.text}</strong> {notification.status} &nbsp;
                {notification.url}
              </>
            )
          } else {
            notificationBody = (<span>{notification.text}</span>)
          }
          return (
            <Alert
              key={notification.id}
              variant={notification.type}
              timeout={true}
              onTimeout={() => { this.props.dispatch(clearNotification(notification.id)) }}
              actionClose={
                <AlertActionCloseButton
                  onClose={() => this.props.dispatch(clearNotification(notification.id))}
                />
              }
            >
              <span title={moment_tz.utc(notification.date).tz(this.props.timezone).format()}>
                {notificationBody}
              </span>
            </Alert>
          )
        }
        )}
      </AlertGroup>
    )
  }

  renderTenantDropdown() {
    const { tenant, tenants } = this.props
    const { isTenantDropdownOpen } = this.state

    if (tenant.whiteLabel) {
      return (
        <PageHeaderToolsItem>
          <strong>Tenant</strong> {tenant.name}
        </PageHeaderToolsItem>
      )
    } else {
      const tenantLink = (_tenant) => {
        const currentPath = this.props.location.pathname
        let suffix
        switch (currentPath) {
          case '/t/' + tenant.name + '/projects':
            suffix = '/projects'
            break
          case '/t/' + tenant.name + '/jobs':
            suffix = '/jobs'
            break
          case '/t/' + tenant.name + '/labels':
            suffix = '/labels'
            break
          case '/t/' + tenant.name + '/nodes':
            suffix = '/nodes'
            break
          case '/t/' + tenant.name + '/autoholds':
            suffix = '/autoholds'
            break
          case '/t/' + tenant.name + '/builds':
            suffix = '/builds'
            break
          case '/t/' + tenant.name + '/buildsets':
            suffix = '/buildsets'
            break
          case '/t/' + tenant.name + '/status':
          default:
            // all other paths point to tenant-specific resources that would most likely result in a 404
            suffix = '/status'
            break
        }
        return <Link to={'/t/' + _tenant.name + suffix}>{_tenant.name}</Link>
      }

      const options = tenants.tenants.filter(
        (_tenant) => (_tenant.name !== tenant.name)
      ).map(
        (_tenant, idx) => {
          return (
            <DropdownItem key={'tenant-dropdown-' + idx} component={tenantLink(_tenant)} />
          )
        })
      options.push(
        <DropdownSeparator key="tenant-dropdown-separator" />,
        <DropdownItem
          key="tenant-dropdown-tenants_page"
          component={<Link to={tenant.defaultRoute}>Go to tenants page</Link>} />
      )

      return (tenants.isFetching ?
        <PageHeaderToolsItem>
          Loading tenants ...
        </PageHeaderToolsItem> :
        <>
          <PageHeaderToolsItem>
            <Dropdown
              isOpen={isTenantDropdownOpen}
              toggle={
                <DropdownToggle
                  className={`zuul-menu-dropdown-toggle${isTenantDropdownOpen ? '-expanded' : ''}`}
                  id="tenant-dropdown-toggle-id"
                  onToggle={(isOpen) => { this.setState({ isTenantDropdownOpen: isOpen }) }}
                  toggleIndicator={ChevronDownIcon}
                >
                  <strong>Tenant</strong> {tenant.name}
                </DropdownToggle>}
              onSelect={() => { this.setState({ isTenantDropdownOpen: !isTenantDropdownOpen }) }}
              dropdownItems={options}
            />
          </PageHeaderToolsItem>
        </>)
    }
  }

  render() {
    const { isKebabDropdownOpen } = this.state
    const { notifications, tenantStatus, tenant, info, auth } = this.props

    const menu = routes(auth.info, this.props.user)
    const nav = this.renderMenu(menu)

    const kebabDropdownItems = []
    if (!info.tenant) {
      kebabDropdownItems.push(
        <DropdownItem
          key="components"
          onClick={event => this.handleComponentsLink(event)}
        >
          <ServiceIcon /> Components
        </DropdownItem>
      )
    }

    kebabDropdownItems.push(
      <DropdownItem key="api" onClick={event => this.handleApiLink(event)}>
        <CodeIcon /> API
      </DropdownItem>
    )
    kebabDropdownItems.push(
      <DropdownItem
        key="documentation"
        onClick={event => this.handleDocumentationLink(event)}
      >
        <BookIcon /> Documentation
      </DropdownItem>
    )

    if (tenant.name) {
      kebabDropdownItems.push(
        <DropdownItem
          key="tenant"
          onClick={event => this.handleTenantLink(event)}
        >
          <UsersIcon /> Tenants
        </DropdownItem>
      )
    }

    const pageHeaderTools = (
      <PageHeaderTools className="zuul-header-tools">
        {/* The utility navbar is only visible on desktop sizes
            and replaced by a kebab dropdown for smaller sizes */}
        <PageHeaderToolsGroup
          visibility={{ default: 'hidden', lg: 'visible' }}
        >
          { (!info.tenant) &&
            <PageHeaderToolsItem>
              <Link to='/components'>
                <Button variant={ButtonVariant.plain}>
                  <ServiceIcon /> Components
                </Button>
              </Link>
            </PageHeaderToolsItem>
          }
          <PageHeaderToolsItem>
            <Link to='/openapi'>
              <Button variant={ButtonVariant.plain}>
                <CodeIcon /> API
              </Button>
            </Link>
          </PageHeaderToolsItem>
          <PageHeaderToolsItem>
            <a
              href='https://zuul-ci.org/docs'
              rel='noopener noreferrer'
              target='_blank'
            >
              <Button variant={ButtonVariant.plain}>
                <BookIcon /> Documentation
              </Button>
            </a>
          </PageHeaderToolsItem>
          {tenant.name && (this.renderTenantDropdown())}
        </PageHeaderToolsGroup>
        <PageHeaderToolsGroup>
          {/* this kebab dropdown replaces the icon buttons and is hidden for
              desktop sizes */}
          <PageHeaderToolsItem visibility={{ lg: 'hidden' }}>
            <Dropdown
              isPlain
              position="right"
              onSelect={this.handleKebabDropdownSelect}
              toggle={<KebabToggle onToggle={this.handleKebabDropdownToggle} />}
              isOpen={isKebabDropdownOpen}
              dropdownItems={kebabDropdownItems}
            />
          </PageHeaderToolsItem>
        </PageHeaderToolsGroup>
        {tenantStatus.config_error_count > 0 &&
          <NotificationBadge
            isRead={false}
            aria-label="Notifications"
          >
            <Link to={this.props.tenant.linkPrefix + '/config-errors'}
                  style={{color: 'white'}}>
              <BellIcon />
            </Link>
          </NotificationBadge>
        }
        <SelectTz />
        <ConfigModal />

        {auth.info && auth.info.default_realm && (<AuthContainer />)}
      </PageHeaderTools>
    )

    // In case we don't have an active tenant, fall back to the root URL
    const logoUrl = tenant.name ? tenant.defaultRoute : '/'
    const pageHeader = (
      <PageHeader
        logo={<Brand src={logo} alt='Zuul logo' />}
        logoProps={{ to: logoUrl }}
        logoComponent={Link}
        headerTools={pageHeaderTools}
      />
    )

    return (
      <React.Fragment>
        {notifications.length > 0 && this.renderNotifications(notifications)}
        <Page className="zuul-page" header={pageHeader} tertiaryNav={nav}>
          <ErrorBoundary>
            {this.renderContent(menu)}
          </ErrorBoundary>
        </Page>
      </React.Fragment>
    )
  }
}

// This connect the info state from the store to the info property of the App.
const App = withRouter(connect(
  state => ({
    notifications: state.notifications,
    tenantStatus: state.tenantStatus.tenant_status,
    tenantStatusReady: state.tenantStatus.ready,
    info: state.info,
    tenant: state.tenant,
    tenants: state.tenants,
    timezone: state.timezone,
    user: state.user,
    auth: state.auth,
  })
)(withAuth(AppComponent)))

export default App
