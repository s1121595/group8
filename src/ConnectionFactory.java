import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties; /**
 * 
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 * 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
*/

public class ConnectionFactory {
	public static final String POSTGRESQL = "postgresql";

	public static Connection getConnection() throws SQLException {
		String url = "jdbc:postgresql://localhost:5432/my_database"; // or your custom database name

		Properties props = new Properties();
		props.setProperty("user", "postgres");
		props.setProperty("password", "password");

		Connection conn = DriverManager.getConnection(url, props);
		conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

		return conn;
	}

	public static Connection getDefaultParameterConnection(String type) throws SQLException {
		return getConnection();
	}
}
