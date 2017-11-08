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

var argv = require('yargs')
.option('task', {
  alias: 't',
  default: 'pull'
}).argv;


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
    .catch(() => {})
    .then(() => {
      args.push('--force');

      return {args, table, db};
    });
  })
  .then((all) => {
    return all;
  });
};

let dump = (tnl, settings) => {
  let args = [
    'dump',
    '-c', `localhost:${settings.tunnel.localPort}`,
    '-e', settings.remoteDb,
    '-f', settings.archive,
    '--password-file', settings.remotePwdFile
  ];
  let rdb = spawn('rethinkdb', args);
  let line = '';

  return new Promise(function(resolve, reject) {
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

let dropOrMergeTable = (connected, table) => {
  if (argv.merge) {
    return Promises.try(() => {
      return console.log(`\nPreparing to merge data into ${table} table…`);
    });
  }

  console.log(chalk.cyan(`\nPreparing to remove ${table} table`));

  return connected
  .then((conn) => {
    return r.tableDrop(table)
    .run(conn)
    .then(function(cursor) {
      return console.log(chalk.cyan(`\nRemoved ${table} table`));
    });
  })
  .catch((args) => {
    console.error(chalk.red(`\nCould not remove table ${table} because it does not exist`));
  });

};

const createDb = (settings) => {
  let connected = r.connect({
    db: settings.localDb,
    password: settings.localPwd
  });

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
  });
};

let restore = (settings) => {
  const decompress = require('decompress');
  const decompressTargz = require('decompress-targz');

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
      let connected = r.connect({
        db: settings.localDb,
        password: settings.localPwd
      });

      return Promises.try(() => {
        return setImportArgs(settings, files);
      })
      .each(({args, table, db}) => {
        return dropOrMergeTable(connected, table)
        .then(() => {
          let rdb = spawn('rethinkdb', args);

          return new Promise(function(resolve, reject) {
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
      })
      .then(() => {
        return connected
        .then((conn) => {
          conn.close();
          console.log('Closing db connection:', chalk.green('Updates complete'));
        })
        .catch((err) => {
          connected.close();
          console.error('Uh oh!');
          console.error(err);
        });
      });
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

    return r.connect({
      db: settings.localDb,
      password: settings.localPwd
    })
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
      r.table('User').run(connection).then(function(cursor) {
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

    return new Promise(function(resolve, reject) {

      tunnel(settings.tunnel, function(err, tnl) {
        if (err) {
          console.log('Error!');
          tnl.close();

          return reject(err);
        }

        return fs.writeFile(settings.remotePwdFile, settings.remotePwd)
        .then(() => {
          return fs.writeFile(settings.localPwdFile, settings.localPwd);
        })
        .then(() => {
          return dump(tnl, settings);
        })
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
    tunnel: {},
    // localDb: '',
    // localPwd: '',
    // remoteDb: '',
    // remotePwd: '',
    //
  }, options);

  argv.merge = !!opts.merge;

  // Need to do this because Object.assign is not recursive:
  opts.tunnel = Object.assign(tunnelConfig, opts.tunnel);

  let remoteDbList = false;

  if (Array.isArray(opts.remoteDb)) {
    remoteDbList = opts.remoteDb.length > 1;
    opts.remoteDb = remoteDbList ? opts.remoteDb : opts.remoteDb[0];
  }

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
      type: remoteDbList ? 'list' : 'input',
      choices: remoteDbList ? opts.remoteDb : null,
      when: () => {
        return remoteDbList || (task === 'pull' && !process.env.REMOTE_DB_NAME && !opts.remoteDb);
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
      when: !opts.localDb
    },
    {
      name: 'localPwd',
      type: 'input',
      message: 'No DB_PASSWORD in .env. What is the admin password for local db?',
      when: !process.env.LOCAL_DB_ADMIN_PASSWORD && !opts.localPwd,
    },
  ];

  let questions = [...required];

  questions.push({
    name: 'confirmOverwrite',
    type: 'confirm',
    message: function(answers) {
      const db = opts.localDb || answers.localDb;

      return `You are about to overwrite tables in the ${db} database. Old data will not be preserved. You sure?`;
    },
    when: !argv.merge,
  });

  return inquirer.prompt(questions)
  .then((answers) => {
    opts = mergeOptionsWithAnswers(opts, answers);

    let stop = false;

    if (!tasks[argv.task]) {
      return console.log(`Cannot run db task ${argv.task}`);
    }

    if (!argv.merge && !answers.confirmOverwrite) {
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

    opts.remotePwdFile = path.join(opts.tempDir, `${opts.remoteDb}-remote.txt`);
    opts.localPwdFile = path.join(opts.tempDir, `${opts.localDb}-local.txt`);

    return fs.ensureDir(opts.tempDir)
    .then(() => {
      console.log(chalk.cyan(`Running ${task}...`));

      return tasks[task](opts);
    });

  });
};

module.exports = runTask;
