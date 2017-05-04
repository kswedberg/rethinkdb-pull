const dotenv = require('dotenv');
const path = require('path');
const spawn = require('child_process').spawn;
const Promises = require('bluebird');
const fs = require('fs-extra');
const inquirer = require('inquirer');
const chalk = require('chalk');
const glob = require('globby');

dotenv.config();

var argv = require('yargs')
.option('task', {
  alias: 't',
  // type: 'boolean',
  default: 'pull'
}).argv;

let mergeOptions = (opts, answers) => {
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
        reject(code);
      } else {
        resolve();
      }
    });
  });
};

let restore = (settings) => {
  const decompress = require('decompress');
  const decompressTargz = require('decompress-targz');

  return decompress(settings.archive, settings.tempDir, {
    plugins: [decompressTargz()]
  }).then(() => {
    console.log(`Decompressed ${settings.archive}`);
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
              reject(code);
            } else {
              console.log(chalk.cyan(`Imported ${table} into ${db}`));
              resolve();
            }
          });
        });
      })
      .then(() => {
        console.log(chalk.green('Updates complete'));
      });
    });
  });
};

let clean = (settings) => {
  fs.remove(settings.tempDir)
  .then(() => {
    console.log(`Removed ${settings.tempDir}`);
  });
};

let tasks = {

  pull: (settings) => {
    let tunnel = require('tunnel-ssh');

    tunnel(settings.tunnel, function(err, tnl) {
      if (err) {
        console.log('Error!');
        tnl.close();

        return console.log(err);
      }

      return fs.writeFile(settings.remotePwdFile, settings.remotePwd)
      .then(() => {
        return fs.writeFile(settings.localPwdFile, settings.localPwd);
      })
      .then(() => {
        return dump(tnl, settings);
      })
      .then(() => {
        return restore(settings);
      })
      .then(() => {
        return clean(settings);
      })
      .catch((code) => {
        console.log(`Process failed with code ${code}`);
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

  // Need to do this because Object.assign is not recursive:
  opts.tunnel = Object.assign(tunnelConfig, opts.tunnel);

  let remoteDbList = false;

  if (Array.isArray(opts.remoteDb)) {
    remoteDbList = opts.remoteDb.length > 1;
    opts.remoteDb = remoteDbList ? opts.remoteDb : opts.remoteDb[0];
  }

  let questions = [
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

  inquirer.prompt(questions)
  .then((answers) => {
    opts = mergeOptions(opts, answers);

    let stop = false;

    if (!tasks[argv.task]) {
      return console.log(`Cannot run db task ${argv.task}`);
    }

    questions.forEach((item) => {
      if (!opts[item.name]) {
        stop = true;
        console.log(chalk.red(`The ${item.name} setting is required.`));
      }
    });

    if (stop) {
      return console.log('Cannot continue');
    }

    opts.remotePwdFile = path.join(opts.tempDir, `${opts.remoteDb}.txt`);
    opts.localPwdFile = path.join(opts.tempDir, `${opts.localDb}.txt`);

    fs.ensureDir(opts.tempDir)
    .then(() => {
      console.log(chalk.yellow(`Running ${task}...`));
      tasks[task](opts);
    });

  });
};

module.exports = runTask;
