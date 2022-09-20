# RethinkDB Pull

**NOTE: This repo is no longer actively developed.** Use at your own risk. If you need something like this, my [db-backup-restore](https://github.com/kswedberg/db-backup-restore) repo might help.

## Features
* Prompts for local and remote db/password info if none provided in `options` argument or `.env` file
* Uses ssh-tunnel to connect to a remote RethinkDB server
* Downloads a compressed file containing a database from the server
* Extracts the file and imports the tables into a local database, overwriting existing data unless `merge` option is `true`

## Requirements

* rethinkdb: The Python version (`sudo pip install rethinkdb`)
* node.js >= 4

## Usage

Put the following in a file within your project (e.g. `pull.js`):
```js
const rethinkdbPull = require('rethinkdb-pull');

// Using minimum required settings
rethinkdbPull({
  tunnel: {
    username: 'somebody',
    host: 'example.com'
  }
})
.then(() => console.log('Hooray, I finished!'));
```

Add an npm script to `package.json`:
```json
{
  "scripts": {
    "db:pull": "node ./pull.js"
  }
}
```

On the command line:
```sh
npm run db:pull
```
## Returns

The `rethinkdbPull` function returns a `Promise`.

The Promise is resolved with a `settings` object, which is the result of merging per-call settings, environment variables, and default settings.

## Settings

Available settings, with their defaults:

```js
let settings = {
  localDb: undefined, // String
  localPwd: undefined, // String
  remoteDb: undefined, // String or Array
  remotePwd: undefined,
  tempDir: '/tmp',
  includeTables: [],
  excludeTables: [],
  force: false, // if true, skips inquirer prompts that would otherwise appear
  fetchOnly: false, // [ *see more below ]
  fetchToPath: '/tmp/rethink_dump.tar.gz', // if fetchOnly is true, path of the tarball to save
  // See https://github.com/agebrock/tunnel-ssh for details on tunnel settings:
  tunnel: {
    username: undefined,
    host: undefined,
    port: 22,
    dstHost: '127.0.0.1',
    dstPort: 28015,
    localHost: '127.0.0.1',
    localPort: 9999,
    keepAlive: true,
  }
}
```

\* `fetchOnly` may be useful as a backup strategy.
  If set to `true`, the function call will only `dump` the remote db, NOT `restore` into a local db. The resulting file will be saved as:  `[tempDir]/[timestamp]/rethink_dump.tar.gz`.

### Database and password settings
* If you don't include values for `localDb`, `localPwd`, `remoteDb`, or `remotePwd`, you will be prompted to do so.
* Instead of passing these settings into the function argument, you may use `.env` settings:
    ```sh
    DB_NAME # localDb
    LOCAL_DB_ADMIN_PASSWORD # localPwd
    REMOTE_DB_NAME # remoteDb
    REMOTE_DB_ADMIN_PASSWORD # remotePwd
* If you pass in `remoteDb` as an array, you will be prompted to choose one of the array elements when the script is run.
