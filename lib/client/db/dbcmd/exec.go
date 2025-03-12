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

package dbcmd

import (
	"context"
	"os/exec"

	"github.com/gravitational/trace"
)

func (c *CLICommandBuilder) getPostgresExecCommand(ctx context.Context, query string) (*exec.Cmd, error) {
	cmd := c.getPostgresCommand()
	cmd.Args = append(cmd.Args, "-c", query)
	return cmd, nil
}

func (c *CLICommandBuilder) getMySQLExecCommand(ctx context.Context, query string) (*exec.Cmd, error) {
	cmd, err := c.getMySQLCommand()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	cmd.Args = append(cmd.Args, "-e", query)
	return cmd, nil
}
