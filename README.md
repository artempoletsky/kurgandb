
## env file settings
You can override global variables by creating an `.env` file in Node's current working directory.

```env
KURGANDB_SERVER_PORT = 8080
```
Specifies port on which the server will be listening. `8080` is default.

```env
KURGANDB_DATA_DIR = "D:/path/to/your/directory"
```
Specifies where the DB will store it's data. If not set it will be `process.cwd() + "/kurgandb_data"`.

```env
KURGANDB_REMOTE_ADDRESS = "http://127.0.0.1:8080"
```
Specifies address and port for the `remoteQuery` method. If not specified the method will throw an error.