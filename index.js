const dotenv = require('dotenv');
const path = require('path');
const spawn = require('child_process').spawn;
const Promises = require('bluebird');
const fs = require('fs-extra');
const r = require('rethinkdb');
const inquirer = require('inquirer');
const chalk = require('chalk');
const glob = require('globby');

dotenv.config();

let argv = require('yargs')
.option('task', {
  alias: 't',
  default: 'pull'
}).argv;


let setOpts = (options) => {
  let tunnelConfig =  {
    port: 22,
    dstHost: '127.0.0.1',
    dstPort: 28015,
    localHost: '127.0.0.1',
    localPort: 9999,
    keepAlive: true,
  };
  let opts = Object.assign({
    tempDir: '/tmp',
    includeTables: [],
    excludeTables: [],
    force: false,
    fetchOnly: false,
    fetchToPath: '/tmp/rethink_dump.tar.gz',
    tunnel: {},
    dbHost: 'localhost'
    // localDb: '',
    // localPwd: '',
    // remoteDb: '',
    // remotePwd: '',
    //
  }, options);

  // Need to do this because Object.assign is not recursive:
  opts.tunnel = Object.assign(tunnelConfig, opts.tunnel);

  if (Array.isArray(opts.remoteDb)) {
    opts.remoteDbList = opts.remoteDb.length > 1;
    opts.remoteDb = opts.remoteDbList ? opts.remoteDb : opts.remoteDb[0];
  }

  return opts;
};

let mergeOptionsWithAnswers = (opts, answers) => {
  opts.remoteDb = opts.remoteDb || answers.remoteDb || process.env.REMOTE_DB_NAME;
  opts.localDb = opts.localDb || answers.localDb || process.env.DB_NAME;
  opts.remotePwd = opts.remotePwd || answers.remotePwd || process.env.REMOTE_DB_ADMIN_PASSWORD;
  opts.localPwd = opts.localPwd || answers.localPwd || process.env.LOCAL_DB_ADMIN_PASSWORD;

  if (Array.isArray(opts.remoteDb)) {
    opts.remoteDb = answers.remoteDb;
  }

  opts.tunnel.username = opts.tunnel.username || answers['tunnel.username'];
  opts.tunnel.host = opts.tunnel.host || answers['tunnel.host'];
  opts['tunnel.username'] = opts.tunnel.username;
  opts['tunnel.host'] = opts.tunnel.host;

  opts.tempDir = path.join(opts.tempDir, `${+new Date()}`);
  opts.archive = path.join(opts.tempDir, 'rethink_dump.tar.gz');

  opts.remotePwdFile = path.join(opts.tempDir, `${opts.remoteDb}-remote.txt`);
  opts.localPwdFile = path.join(opts.tempDir, `${opts.localDb}-local.txt`);

  return opts;
};

let setImportArgs = (settings, files) => {
  let filtered = [...files];

  if (settings.includeTables.length) {
    filtered = files.filter((item) => settings.includeTables.includes(path.basename(item, '.json')));
  } else if (settings.excludeTables.length) {
    filtered = files.filter((item) => !settings.excludeTables.includes(path.basename(item, '.json')));
  }

  return Promises.map(filtered, (file) => {
    let infoFile = file.replace(/\.json$/, '.info');
    let db = settings.localDb;
    let table = path.basename(file, '.json');

    let args = [
      'import',
      '-c', `${settings.dbHost}:${settings.port || settings.tunnel.dstPort}`,
      '-f', file,
      '--password-file', settings.localPwdFile,
      '--table', `${db}.${table}`,
      '--format', 'json',
    ];

    return fs.readJson(infoFile)
    .then((json) => {
      if (json.primary_key) {
        args.push('--pkey', json.primary_key);
      }
    })
    .catch(() => {
      // keep going.
    })
    .then(() => {
      args.push('--force');

      return {args, table, db};
    });
  })
  .then((all) => {
    return all;
  });
};

const connectDb = (settings) => {
  return r.connect({
    host: settings.dbHost,
    db: settings.localDb,
    password: settings.localPwd
  });
};

let dump = (tnl, settings) => {
  let args = [
    'dump',
    '-c', `${settings.dbHost}:${settings.port || settings.tunnel.localPort}`,
    '-e', settings.remoteDb,
    '-f', settings.archive,
    '--password-file', settings.remotePwdFile
  ];
  let rdb = spawn('rethinkdb', args);
  let line = '';

  return new Promise((resolve, reject) => {
    rdb.stdout.on('data', (data) => {
      if (line.slice(0, 1) === '[') {
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
      }
      line = data.toString();
      process.stdout.write(line);
    });

    rdb.stderr.on('data', (data) => {
      console.log('stderr', data.toString());
    });

    rdb.on('close', (code) => {

      if (typeof tnl !== 'undefined') {
        tnl.close();
      }

      if (code) {
        const err = new Error(`dump function: ${code}`);

        reject(err);
      } else {
        resolve();
      }
    });
  });
};

let saveFetchedFile = (settings) => {
  return fs.copy(settings.archive, settings.fetchToPath)
  .then(() => {
    return settings;
  });
};

let dropOrMergeTable = (connected, table) => {

  console.log(chalk.cyan(`\nPreparing to remove ${table} table`));

  return connected
  .then((conn) => {
    return r.tableDrop(table)
    .run(conn)
    .then((cursor) => {
      return console.log(chalk.cyan(`\nRemoved ${table} table`));
    });
  })
  .catch((args) => {
    console.log(chalk.yellow(`\nCould not remove table ${table} because it does not exist`));
  });

};

const createDb = (settings) => {
  let connected = connectDb(settings);

  return connected
  .then((connection) => {
    return r.dbList()
    .run(connection)
    .then((dbs) => {
      return {
        connection,
        hasDb: dbs.includes(settings.localDb),
      };
    });
  })
  .then(({connection, hasDb}) => {
    if (hasDb) {
      return connection;
    }

    console.log('Creating', settings.localDb);

    return r
    .dbCreate(settings.localDb)
    .run(connection)
    .then(() => {
      connection.use(settings.localDb);

      return connection;
    });
  })
  .then((connection) => {
    connection.close();
  });
};

let restore = (settings) => {
  const decompress = require('decompress');
  const decompressTargz = require('decompress-targz');

  let connected = connectDb(settings);

  return decompress(settings.archive, settings.tempDir, {
    plugins: [decompressTargz()]
  }).then(() => {
    console.log(chalk.yellow(`Decompressed ${settings.archive}`));
  })
  .then(() => {
    return glob([
      path.join(settings.tempDir, '**/*.json')
    ])
    .then((files) => {
      return Promises.try(() => {
        return setImportArgs(settings, files);
      })
      .each(({args, table, db}) => {
        return dropOrMergeTable(connected, table)
        .then(() => {
          let rdb = spawn('rethinkdb', args);

          return new Promise((resolve, reject) => {
            rdb.stdout.on('data', (data) => {
              let line = data.toString();

              process.stdout.write(line);
            });

            rdb.stderr.on('data', (data) => {
              console.log('stderr:', chalk.red(data.toString()));
            });

            rdb.on('close', (code) => {

              if (code) {
                const err = new Error(`restore function: ${code}`);

                reject(err);
              } else {
                console.log(chalk.yellow(`Imported ${table} into ${db}`));
                resolve();
              }
            });
          });
        });
      });
    });
  })
  .then(() => {
    return connected
    .then((conn) => {
      conn.close();
      console.log('Closing db connection:', chalk.green('Updates complete'));

      return settings;
    })
    .catch((err) => {
      console.error('Uh oh!');
      console.error(err);

      return connected.then((conn) => conn.close());
    });
  });
};

let clean = (settings) => {
  return fs.remove(settings.tempDir)
  .then(() => {
    console.log(`Housekeeping: Removed ${settings.tempDir}`);

    return settings;
  });
};

let tasks = {

  test: (settings) => {

    return connectDb(settings)
    .then((connection) => {
      return r.dbList()
      .run(connection)
      .then((dbs) => {
        return {
          connection,
          hasDb: dbs.includes(settings.localDb),
        };
      });
    })
    .then(({connection, hasDb}) => {
      if (hasDb) {
        return connection;
      }

      console.log('Creating', settings.localDb);

      return r
      .dbCreate(settings.localDb)
      .run(connection)
      .then(() => {
        connection.use(settings.localDb);

        return connection;
      });
    })
    .then((connection) => {
      return r.table('User').run(connection).then((cursor) => {
        return cursor.toArray();
      })
      .then((result) => {
        console.log(result);
      });
    })
    .catch(console.error);

  },
  pull: (settings) => {
    let tunnel = require('tunnel-ssh');

    return new Promise((resolve, reject) => {

      tunnel(settings.tunnel, (err, tnl) => {
        if (err) {
          console.log('Error!');
          tnl.close();

          return reject(err);
        }

        const pulling = fs.writeFile(settings.remotePwdFile, settings.remotePwd)
        .then(() => {
          return fs.writeFile(settings.localPwdFile, settings.localPwd);
        })
        .then(() => {
          return dump(tnl, settings);
        });

        if (settings.fetchOnly) {
          return pulling
          .then(() => {
            return saveFetchedFile(settings);
          })
          .then(() => {
            return clean(settings)
            .then(resolve);
          });
        }

        return pulling
        .then(() => {
          return createDb(settings);
        })
        .then(() => {
          return restore(settings);
        })
        .then(() => {
          return clean(settings)
          .then(resolve);
        })
        .catch((err) => {
          console.log('Process failed.');

          return clean(settings)
          .then(() => {
            return reject(err);
          });
        });

      });
    });
  },

};


let runTask = function runTask(options = {}) {
  let task = argv.task || 'pull';

  let opts = setOpts(options);

  let required = [
    {
      name: 'tunnel.username',
      message: 'ssh tunnel username?',
      when: !opts.tunnel.username,
    },
    {
      name: 'tunnel.host',
      message: 'ssh tunnel host?',
      when: !opts.tunnel.host,
    },
    {
      name: 'remoteDb',
      message: 'Which remote DB do you want to pull?',
      type: opts.remoteDbList ? 'list' : 'input',
      choices: opts.remoteDbList ? opts.remoteDb : null,
      when: () => {
        return opts.remoteDbList || (task === 'pull' && !process.env.REMOTE_DB_NAME && !opts.remoteDb);
      },
    },
    {
      name: 'remotePwd',
      type: 'input',
      message: 'No REMOTE_DB_ADMIN_PASSWORD in .env. What is the admin password for remote db?',
      when: !process.env.REMOTE_DB_ADMIN_PASSWORD && !opts.remotePwd,
    },
    {
      name: 'localDb',
      type: 'input',
      message: 'Which local DB do you want to overwrite?',
      default: process.env.DB_NAME || '',
      when: !opts.localDb && !opts.fetchOnly && !opts.force
    },
    {
      name: 'localPwd',
      type: 'input',
      message: 'No DB_PASSWORD in .env. What is the admin password for local db?',
      when: !process.env.LOCAL_DB_ADMIN_PASSWORD && !opts.localPwd,
    },
  ];

  let questions = [...required];
  let other = [
    {
      name: 'confirmOverwrite',
      type: 'confirm',
      message: function(answers) {
        const db = opts.localDb || answers.localDb;

        return `You are about to overwrite tables in the ${db} database. Old data will not be preserved. You sure?`;
      },
      when: !opts.fetchOnly && !opts.force,
    },
    {
      name: 'confirmFetchOnly',
      type: 'confirm',
      message: `Do you really want to save the remote db to the following file?
${opts.fetchToPath}
`,
      when: opts.fetchOnly && !opts.force
    }
  ];

  questions.push(...other);

  return inquirer.prompt(questions)
  .then((answers) => {
    opts = mergeOptionsWithAnswers(opts, answers);

    let stop = false;
    let bail = opts.fetchOnly ? !answers.confirmFetchOnly && !opts.force : !answers.confirmOverwrite && !opts.force;

    if (!tasks[argv.task]) {
      return console.log(`Cannot run db task ${argv.task}`);
    }

    if (bail) {
      return console.log(chalk.red('Okay, not going to continue'));
    }

    required.forEach((item) => {
      if (!opts[item.name]) {
        stop = true;
        console.log(chalk.red(`The ${item.name} setting is required.`));
      }
    });

    if (stop) {
      return console.log('Cannot continue');
    }

    return fs.ensureDir(opts.tempDir)
    .then(() => {
      console.log(chalk.cyan(`Running ${task}...`));

      return tasks[task](opts);
    });

  });
};

module.exports = runTask;

const setLocalOpts = (options) => {
  let opts = setOpts(options);

  opts.remoteDb = opts.db || opts.fromDb || opts.remoteDb;
  opts.localDb = opts.toDb || opts.localDb;
  let settings = mergeOptionsWithAnswers(opts, {});

  settings = Object.assign({
    force: true,
    port: 28015,
  }, settings);

  settings.remotePwd = settings.pwd || settings.localPwd;

  return settings;
};

const dumpLocal = (settings) => {
  return fs.ensureDir(settings.tempDir)
  .then(() => {
    fs.writeFile(settings.remotePwdFile, settings.remotePwd);
  })
  .then(() => {
    return fs.writeFile(settings.localPwdFile, settings.localPwd);
  })
  .then(() => {
    return dump(undefined, settings);
  });
};

// BACKUP LOCAL: backs up db from local rethinkdb server to a path on the local filesystem
module.exports.backupLocal = (options = {}) => {
  let settings = setLocalOpts(options);

  // backupLocal uses a local database
  // but the dump() function above uses settings.remoteDb.
  // so we need to ensure that we're using settings.localDb if .db or .fromDb not set
  settings.remoteDb = settings.db || settings.fromDb || settings.localDb;
  settings.localDb = 'notused';

  return dumpLocal(settings)
  .then(() => {
    return saveFetchedFile(settings);
  })
  .then(() => {
    return clean(settings);
  })
  .catch(console.error);
};

// SYNC LOCAL: syncs from one db to another on the same server
module.exports.syncLocal = (options = {}) => {
  let settings = setLocalOpts(options);

  // return console.log(settings);
  return dumpLocal(settings)
  .then(() => {
    return createDb(settings);
  })
  .then(() => {
    return restore(settings);
  })
  .then(() => {
    return clean(settings);
  })
  .catch((err) => {
    console.log('Process failed.');
    console.error(err);

    return clean(settings);
  });
};
