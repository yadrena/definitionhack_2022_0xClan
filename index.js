// index.js

const Koa = require('koa')
const Router = require('@koa/router')
const json = require('koa-json')
const koaBody = require('koa-body');
const settings = require('./settings').mainnet;
const abis = require('./abis');

const app = new Koa()
const router = new Router()
const port = process.env.PORT || 4000
const Web3 = require('web3');
const axios = require("axios");
const crypto = require("crypto");
const fs = require("fs");
const abiDecoder = require('abi-decoder');
const sqlite = require("aa-sqlite");

const {createLogger, format, transports, winston} = require("winston");
require('winston-daily-rotate-file');

let transport = new transports.DailyRotateFile({
    filename: './logs/%DATE%.log',
    datePattern: 'YYYY-MM-DD-HH',
    zippedArchive: true,
    maxSize: '20m',
    maxFiles: '14d'
});

let logger = createLogger({
    format: format.combine(format.timestamp(), format.json()),
    transports: [transport, new transports.Console()],
    exceptionHandlers: [new transports.File({filename: "./logs/exceptions.log"})],
    rejectionHandlers: [new transports.File({filename: "./logs/rejections.log"})],
});

let web3, currentBlock;

app
    .use(koaBody())
    .use(router.routes())
    .use(router.allowedMethods())
    .use(json({pretty: true}))


async function createWeb3() {
    try {
        web3 = new Web3(new Web3.providers.HttpProvider(settings.vars.provider));
        currentBlock = await web3.eth.getBlockNumber();
        abiDecoder.addABI(abis.squid.squid);
        return true;
    } catch (error) {
        logger.error("createWeb3", error);
        return false;
    }
}

app.listen(port)

async function main() {
    await sqlite.open('./rating.sqlite');
    if (!await createWeb3()) {
        return false;
    }
    logger.info("Started");
}

router.get('/stats/:player', async (ctx) => {
    let player = ctx.params.player;
    if (!player) {
        //todo err
        player = 'nan';
    }
    ctx.body = await getPlayerStats(player);
})

/**
 * This should be used to download archived data or after restart.
 * In production mode all data should be gathered and updated in realtime (not implemented)
 */
router.get('/parser', async (ctx) => {
    let cnt = 1;
    for (let block = settings.squid.block; block < currentBlock; block = block + 10000) {
        let url = 'https://api.bscscan.com/api?module=account&action=txlist&address=' + settings.squid.contract + '&startblock=' + block + '&endblock=' + (block + 10000) + '&page=1&offset=10000&sort=asc&apikey=' + settings.vars.bscAPI;
        logger.info("Downloading...", {'Block': block, 'URL': url});
        let body = await getURL(url);
        body = JSON.parse(body);

        if (body && parseInt(body.status) === 1) {
            let r = body.result;
            for (let row of r) {
                if (row.input.substring(0, 9) === '0x102f211') {
                    console.log(cnt++);
                    await parsePlayTransaction(row);
                }
            }

        }
    }
    //todo market parsing for additional analysis
    /*
    for (let block = settings.biswap.marketplaceBlock; block < 15396346; block = block + 10000) {
        let url = 'https://api.bscscan.com/api?module=account&action=txlist&address=' + settings.biswap.marketplace + '&startblock=' + block + '&endblock=' + (block + 10000) + '&page=1&offset=10000&sort=asc&apikey=' + settings.vars.bscAPI;
        logger.info("Downloading...",{'Block': block, 'URL': url});
        let body = await getURL(url);
    }

     */

    ctx.body = 'OK';

})

async function parsePlayTransaction(row) {
    let data = await cachedTransactionInfo(row.hash);
    if (!data || !data.logs || data.logs.length === 0) {
        return false;
    }
    let players = data.data[1].find(o => o.name === '_playersId').value;
    let log = {};
    log.gameIndex = parseInt(data.logs[0].events.find(o => o.name === 'gameIndex').value);
    log.userWin = data.logs[0].events.find(o => o.name === 'userWin').value === true ? 1 : 0;
    log.rewardTokens = data.logs[0].events.find(o => o.name === 'rewardTokens').value;
    log.rewardAmount = data.logs[0].events.find(o => o.name === 'rewardAmount').value;
    try {
        await sqlite.push('insert into games (id,player,game_id,date,win) values (?,?,?,?,?)', [row.hash, data.transaction.from, log.gameIndex, row.timeStamp, log.userWin]);
    } catch (e) {
        //probably exists
        return false;
    }
    for (let player of players) {
        await sqlite.push('insert into games_players (games_id,player_id) values (?,?)', [row.hash, player]);
    }
    for (let i in log.rewardTokens) {
        await sqlite.push('insert into games_rewards (games_id,token,amount) values (?,?,?)', [row.hash, log.rewardTokens[i], log.rewardAmount[i]]);
    }
}

async function rebuildPlayerStats(player) {
    await sqlite.run('delete from player_stats where id=\'' + player + '\'');
    let stats = await sqlite.get_all('select sum(win) as win,count(*) as total,game_id from games where player=? group by game_id', [player]);
    if (stats && ('data' in stats)) {
        for (let row of stats.data) {
            sqlite.push('insert into player_stats (id,game_id,win,total,ratio) values (?,?,?,?,?)', [player, row.game_id, row.win, row.total, row.win / row.total]);
        }
    }
}

async function getPlayerStats(player) {
    let out = {};
    //todo remove from here, should be triggered after each player game
    await rebuildPlayerStats(player);
    let stats = await sqlite.get_all('select * from player_stats where id=?', [player]);
    let total = 0;
    let wins = 0
    if (stats && ('data' in stats)) {
        for (let row of stats.data) {
            total += row.total;
            wins += row.win;
        }
    }
    if (total > 0) {
        out.total = {};
        out.total.plays = total;
        out.total.wins = wins;
        out.total.ratio = Math.round(wins / total * 100) / 100;
        let nft = await sqlite.get_all('select distinct player_id from games_players p, games g where g.id=p.games_id and g.player=?', [player]);
        out.nfts = nft.data;
    }
    out.player = player;
    out.stats = stats.data;
    //todo should be stored in DB for analysis
    out.currentNFT = await getPlayerNFT(player);
    out.balance = 0;
    if (out.currentNFT.length > 0) {
        let total = 0;
        for (let row of out.currentNFT) {
            total += (parseInt(row[1]) + 1);
        }
        out.balance = Math.round(total / out.currentNFT.length);
    }
    out.won = await getPlayerWonAmounts(player);
    //todo calculate real data, can't do it right now bc this requires plenty of time to download
    out.regulars = Math.floor(Math.random() * 40);
    out.unusedNFT = Math.floor(Math.random() * 15);
    return out;
}

async function getPlayerNFT(player) {
    let contract = new web3.eth.Contract(abis.player.player, settings.squid.player);
    return await contract.methods.arrayUserPlayers(player).call();
}

async function getPlayerWonAmounts(player) {
    let data = await sqlite.get_all('select token,sum(amount/1e18) as sum from games_rewards r, games g where g.id=r.games_id and g.player=? group by token', [player]);
    if (data && ('data' in data)) {
        return data.data;
    }
    return [];
}

async function getTransaction(hash) {
    let trans;
    try {
        trans = await web3.eth.getTransaction(hash);
    } catch (e) {
        logger.error('Can not get transaction by hash', hash);
        trans = false;
    }
    return trans;
}

function parseTx(input) {
    if (input == '0x')
        return ['0x', []]
    try {
        let decodedData = abiDecoder.decodeMethod(input);
        if (!decodedData || !('name' in decodedData)) {
            return false;
        }
        let method = decodedData['name'];
        let params = decodedData['params'];
        return [method, params]
    } catch (error) {
        logger.error('Could not parse TX', error);
        return false;
    }
}

function error(code = 1, text = 'Unknown error') {

    return {
        code: code,
        message: text
    }
}

/**
 * Simple wrapper with caching
 * @param url
 * @param ttl
 * @returns {Promise<string|boolean|any>}
 */
async function getURL(url, ttl = 30 * 24 * 60 * 60) {
    let hash = crypto.createHash('md5').update(url).digest('hex');
    let dir = './cache/' + hash.substring(0, 2);
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }
    const path = dir + '/' + hash + '.html';
    if (fs.existsSync(path)) {
        let stats = fs.statSync(path);
        let seconds = (new Date().getTime() - stats.mtime) / 1000;
        if (seconds < ttl) {
            return fs.readFileSync(path, 'utf8')
        }
    }
    let result = await axios.get(url, {transformResponse: []});
    if (result && result.data) {
        fs.writeFileSync(path, result.data);
        return result.data;
    }
    return false;

}

async function cachedTransactionInfo(hash) {
    let dir = './cache/transactions/' + hash.substring(2, 4);
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }
    const path = dir + '/' + hash + '.json';
    if (fs.existsSync(path)) {
        return JSON.parse(fs.readFileSync(path, 'utf8'));
    }
    let result = {};
    try {
        result['transaction'] = await getTransaction(hash);
        result['data'] = await parseTx(result['transaction']['input']);
        result['receipt'] = await web3.eth.getTransactionReceipt(hash);
        result['logs'] = abiDecoder.decodeLogs(result['receipt'].logs);
    } catch (e) {
        logger.error('Parsing error', e);
        return false;
    }
    fs.writeFileSync(path, JSON.stringify(result, null, 2));
    return result;

}

main();