/*
 * Teleport
 * Copyright (C) 2025  Gravitational, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package common

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gravitational/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/gravitational/teleport"
	apiclient "github.com/gravitational/teleport/api/client"
	"github.com/gravitational/teleport/api/client/proto"
	apidefaults "github.com/gravitational/teleport/api/defaults"
	"github.com/gravitational/teleport/api/types"
	"github.com/gravitational/teleport/lib/client"
	"github.com/gravitational/teleport/lib/client/db/dbcmd"
	"github.com/gravitational/teleport/lib/srv/alpnproxy"
	"github.com/gravitational/teleport/lib/tlsca"
	"github.com/gravitational/teleport/lib/utils"
	logutils "github.com/gravitational/teleport/lib/utils/log"
	"github.com/gravitational/teleport/tool/common"
)

type databaseExecCommand struct {
	cf            *CLIConf
	tc            *client.TeleportClient
	clusterClient *client.ClusterClient
	profile       *client.ProfileStatus
	tracer        oteltrace.Tracer
	rootCluster   string
}

func (c *databaseExecCommand) run(cf *CLIConf) error {
	if err := c.init(cf); err != nil {
		return trace.Wrap(err)
	}
	defer c.close()

	dbs, err := c.getDatabases()
	if err != nil {
		return trace.Wrap(err)
	}

	group, groupCtx := errgroup.WithContext(c.cf.Context)
	group.SetLimit(c.cf.MaxConnections)
	for _, db := range dbs {
		group.Go(func() error {
			return trace.Wrap(c.exec(groupCtx, db))
		})
	}
	if err := group.Wait(); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (c *databaseExecCommand) init(cf *CLIConf) (err error) {
	if err := c.checkFlags(cf); err != nil {
		return trace.Wrap(err)
	}

	c.cf = cf
	c.tracer = c.cf.TracingProvider.Tracer(teleport.ComponentTSH)
	c.tc, err = makeClient(cf)
	if err != nil {
		return trace.Wrap(err)
	}

	if err := client.RetryWithRelogin(cf.Context, c.tc, func() error {
		c.clusterClient, err = c.tc.ConnectToCluster(cf.Context)
		return trace.Wrap(err)
	}); err != nil {
		return trace.Wrap(err)
	}

	c.profile, err = c.tc.ProfileStatus()
	if err != nil {
		return trace.Wrap(err)
	}

	c.rootCluster, err = c.tc.RootClusterName(cf.Context)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (c *databaseExecCommand) close() {
	if c.clusterClient != nil {
		if err := c.clusterClient.Close(); err != nil && !trace.IsConnectionProblem(err) {
			logger.WarnContext(c.cf.Context, "Failed to close cluster client", "error", err)
		}
	}
}

func (c *databaseExecCommand) checkFlags(cf *CLIConf) error {
	if cf.MaxConnections < 1 && cf.MaxConnections > 10 {
		return trace.BadParameter("--max-connections must be between 1 and 10")
	}

	// selection flags
	byNames := cf.DatabaseServices != ""
	bySearch := cf.SearchKeywords != "" || cf.Labels != ""
	switch {
	case !byNames && !bySearch:
		return trace.BadParameter("please provide one of --dbs, --labels, --search flag")
	case byNames && bySearch:
		return trace.BadParameter("--labels/--search flags cannot be used with --dbs flag")
	}
	return nil
}

func (c *databaseExecCommand) getDatabases() ([]types.Database, error) {
	if c.cf.DatabaseServices != "" {
		return c.getDatabasesByNames()
	}
	return c.searchDatabases()
}

func (c *databaseExecCommand) getDatabasesByNames() ([]types.Database, error) {
	group, groupCtx := errgroup.WithContext(c.cf.Context)
	group.SetLimit(c.cf.MaxConnections)

	var (
		mu  sync.Mutex
		dbs []types.Database
	)
	for _, name := range strings.Split(c.cf.DatabaseServices, ",") {
		group.Go(func() error {
			list, err := c.listDatabasesWithFilter(groupCtx, &proto.ListResourcesRequest{
				Namespace:           apidefaults.Namespace,
				ResourceType:        types.KindDatabaseServer,
				PredicateExpression: makeDiscoveredNameOrNamePredicate(name),
			})
			if err != nil {
				return trace.Wrap(err)
			}
			switch len(list) {
			case 0:
				return trace.NotFound("database %q not found", name)
			case 1:
				mu.Lock()
				defer mu.Unlock()
				dbs = append(dbs, list[0])
				return nil
			default:
				return trace.CompareFailed("expecting one database but got %d", len(list))
			}
		})
	}

	if err := group.Wait(); err != nil {
		return nil, trace.Wrap(err)
	}
	logger.DebugContext(c.cf.Context, "Fetched database services by names.", "databases", logutils.IterAttr(types.ResourceNameIter(dbs)))
	return dbs, nil
}

func (c *databaseExecCommand) searchDatabases() (databases []types.Database, err error) {
	dbs, err := c.listDatabasesWithFilter(c.cf.Context, c.tc.ResourceFilter(types.KindDatabaseServer))
	if err != nil {
		return nil, trace.Wrap(err)
	}

	logger.DebugContext(c.cf.Context, "Fetched database services with search filter.",
		"databases", logutils.IterAttr(types.ResourceNameIter(dbs)),
	)

	// Print results and prompt for confirmation.
	fmt.Fprintf(c.cf.Stdout(), "Found %d databases:\n\n", len(dbs))
	var rows []databaseTableRow
	for _, db := range dbs {
		rows = append(rows, getDatabaseRow("", "", "", db, nil, nil, false))
	}
	printDatabaseTable(printDatabaseTableConfig{
		writer:         c.cf.Stdout(),
		rows:           rows,
		includeColumns: []string{"Name", "Protocol", "Description", "Labels"},
	})

	if err := c.cf.PromptConfirmation("Do you want to proceed?"); err != nil {
		return nil, trace.Wrap(err)
	}
	return dbs, nil
}

func (c *databaseExecCommand) listDatabasesWithFilter(ctx context.Context, filter *proto.ListResourcesRequest) (databases []types.Database, err error) {
	ctx, span := c.tracer.Start(
		ctx,
		"listDatabasesWithFilter",
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
	)
	defer span.End()

	servers, err := apiclient.GetAllResources[types.DatabaseServer](ctx, c.clusterClient.AuthClient, filter)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	// Pre-checks
	dbs := types.DatabaseServers(servers).ToDatabases()
	for _, db := range dbs {
		if isDatabaseUserRequired(db.GetProtocol()) && c.cf.DatabaseUser == "" {
			return nil, trace.BadParameter("--db-user is required for database %s", db.GetName())
		}
		if isDatabaseNameRequired(db.GetProtocol()) && c.cf.DatabaseName == "" {
			return nil, trace.BadParameter("--db-name is required for database %s", db.GetName())
		}
	}
	return dbs, nil
}

func (c *databaseExecCommand) exec(ctx context.Context, db types.Database) (err error) {
	displayName := common.FormatResourceName(db, false)
	outputWriter := c.cf.Stdout()
	errWriter := c.cf.Stderr()
	defer func() {
		switch {
		// nothing to do if no error.
		case err == nil:

		// Stop-on-error.
		case c.cf.StopOnError:
			fmt.Fprintln(c.cf.Stdout(), "Aborting since --stop-on-error is set.")

		// Continue-on-error.
		default:
			fmt.Fprintln(errWriter, err)
			if c.cf.OutputDir != "" {
				fmt.Fprintf(c.cf.Stderr(), "Failed to execute command for %q. See output file for more details.\n", displayName)
			}
			err = nil
		}
	}()

	switch {
	case c.cf.OutputDir != "":
		// Use full-name instead of display name for output path.
		logFile, err := c.openOutputFile(db.GetName())
		if err != nil {
			return trace.Wrap(err)
		}
		outputWriter = logFile
		errWriter = logFile
		fmt.Fprintf(c.cf.Stdout(), "Executing command for %q. Output and errors will be saved at %q.\n", displayName, logFile.Name())
	default:
		if c.cf.MaxConnections > 1 {
			outputWriter = newDBPrefixWriter(c.cf.Stdout(), displayName)
			errWriter = newDBPrefixWriter(c.cf.Stderr(), displayName)
		} else {
			// Print an extra empty line when running sequentially.
			fmt.Fprintln(c.cf.Stdout(), "")
		}
		fmt.Fprintf(c.cf.Stdout(), "Executing command for %q.\n", displayName)
	}

	dbInfo := &databaseInfo{
		RouteToDatabase: tlsca.RouteToDatabase{
			ServiceName: db.GetName(),
			Protocol:    db.GetProtocol(),
			Username:    c.cf.DatabaseUser,
			Database:    c.cf.DatabaseName,
			Roles:       requestedDatabaseRoles(c.cf),
		},
		database: db,
	}

	lp, err := c.startLocalProxy(ctx, dbInfo)
	if err != nil {
		return trace.Wrap(err)
	}
	defer lp.Close()

	dbCmd, err := c.makeCommand(ctx, dbInfo, lp.GetAddr())
	if err != nil {
		return trace.Wrap(err)
	}
	dbCmd.Stdout = outputWriter
	dbCmd.Stderr = errWriter

	logger.DebugContext(ctx, "Executing database command", "command", dbCmd, "db", db.GetName())
	return c.cf.RunCommand(dbCmd)
}

func (c *databaseExecCommand) startLocalProxy(ctx context.Context, dbInfo *databaseInfo) (*alpnproxy.LocalProxy, error) {
	clientCert, err := c.issueCert(ctx, dbInfo)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	listener, err := createLocalProxyListener("localhost:0", dbInfo.RouteToDatabase, c.profile)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	opts := []alpnproxy.LocalProxyConfigOpt{
		alpnproxy.WithDatabaseProtocol(dbInfo.Protocol),
		alpnproxy.WithClusterCAsIfConnUpgrade(ctx, c.tc.RootClusterCACertPool),
		alpnproxy.WithClientCert(clientCert),
	}

	lpConfig := makeBasicLocalProxyConfig(ctx, c.tc, listener, c.cf.InsecureSkipVerify)
	lp, err := alpnproxy.NewLocalProxy(lpConfig, opts...)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	go func() {
		defer listener.Close()
		if err := lp.Start(ctx); err != nil {
			logger.ErrorContext(ctx, "Failed to start local proxy", "error", err)
		}
	}()
	return lp, nil
}

func (c *databaseExecCommand) issueCert(ctx context.Context, dbInfo *databaseInfo) (tls.Certificate, error) {
	params := client.ReissueParams{
		RouteToCluster: c.tc.SiteName,
		RouteToDatabase: proto.RouteToDatabase{
			ServiceName: dbInfo.RouteToDatabase.ServiceName,
			Protocol:    dbInfo.RouteToDatabase.Protocol,
			Username:    dbInfo.RouteToDatabase.Username,
			Database:    dbInfo.RouteToDatabase.Database,
			Roles:       dbInfo.RouteToDatabase.Roles,
		},
		AccessRequests: c.profile.ActiveRequests,
	}

	keyRing, _, err := c.clusterClient.IssueUserCertsWithMFA(ctx, params)
	if err != nil {
		return tls.Certificate{}, trace.Wrap(err)
	}
	dbCert, err := keyRing.DBTLSCert(dbInfo.RouteToDatabase.ServiceName)
	return dbCert, trace.Wrap(err)
}

func (c *databaseExecCommand) makeCommand(ctx context.Context, dbInfo *databaseInfo, lpAddr string) (*exec.Cmd, error) {
	// TODO(greedy52) support a generic template like:
	// TELEPORT_UNSTABLE_DB_EXEC_COMMAND_TEMPLATE='myscript --user {{.db_user}} --port {{.db_port}} --host {{.db_host}} --exec "{{.db_query}}"'
	addr, err := utils.ParseAddr(lpAddr)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	opts := []dbcmd.ConnectCommandFunc{
		dbcmd.WithLocalProxy("localhost", addr.Port(0), ""),
		dbcmd.WithNoTLS(),
		dbcmd.WithLogger(logger),
		dbcmd.WithGetDatabaseFunc(dbInfo.getDatabaseForDBCmd),
	}
	if opts, err = maybeAddDBUserPassword(c.cf, c.tc, dbInfo, opts); err != nil {
		return nil, trace.Wrap(err)
	}
	if opts, err = maybeAddGCPMetadata(c.cf.Context, c.tc, dbInfo, opts); err != nil {
		return nil, trace.Wrap(err)
	}

	cmd, err := dbcmd.NewCmdBuilder(c.tc, c.profile, dbInfo.RouteToDatabase, c.rootCluster, opts...).
		GetExecCommand(ctx, c.cf.DatabaseQuery)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return cmd, nil
}

func (c *databaseExecCommand) openOutputFile(dbServiceName string) (*os.File, error) {
	logFilePath := filepath.Join(c.cf.OutFile, dbServiceName+".output")
	logFilePath, err := utils.EnsureLocalPath(logFilePath, "", "")
	if err != nil {
		return nil, trace.Wrap(err)
	}
	logFile, err := os.Create(logFilePath)
	return logFile, trace.ConvertSystemError(err)
}
