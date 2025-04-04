/**
 * This file is part of core.
 *
 * core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.hobbit.core.data;

import java.util.concurrent.Semaphore;

public class ContainerTermination {

    @Deprecated
    private Semaphore isTerminated = new Semaphore(0);
    private boolean terminated = false;
    private int exitCode = 0;

    public void notifyTermination(int exitCode) {
        this.exitCode = exitCode;
        terminated = true;
        isTerminated.release();
    }

    @Deprecated
    public void waitForTermination() throws InterruptedException {
        isTerminated.acquire();
    }

    public boolean isTerminated() {
        return terminated;
    }

    public int getExitCode() {
        return exitCode;
    }
}
