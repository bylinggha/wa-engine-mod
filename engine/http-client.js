import axios from 'axios';
import http from 'http';
import https from 'https';

const httpAgent = new http.Agent({
  keepAlive: true,
  maxSockets: 2048,
  keepAliveMsecs: 15000,
});

const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 2048,
  keepAliveMsecs: 15000,
});

axios.defaults.httpAgent = httpAgent;
axios.defaults.httpsAgent = httpsAgent;
axios.defaults.timeout = Math.min(axios.defaults.timeout || 0, 7000) || 7000;
axios.defaults.headers = axios.defaults.headers || {};
axios.defaults.headers.common = axios.defaults.headers.common || {};
axios.defaults.headers.common['Connection'] = 'keep-alive';

// Also apply to Node's global agents for any non-axios HTTP usage
http.globalAgent.keepAlive = true;
http.globalAgent.maxSockets = Math.max(http.globalAgent.maxSockets || 0, 2048);
https.globalAgent.keepAlive = true;
https.globalAgent.maxSockets = Math.max(https.globalAgent.maxSockets || 0, 2048);

export default axios;


