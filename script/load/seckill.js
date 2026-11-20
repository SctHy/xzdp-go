import http from 'k6/http';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import { check, fail, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const file = __ENV.SALE_FIXTURE;
const users = new SharedArray('local sessions', () => JSON.parse(open(file)).users);
const config = new SharedArray('activity', () => {
  const {voucher, stock} = JSON.parse(open(file));
  return [{voucher, stock}];
})[0];
const base = __ENV.BASE_URL || 'http://127.0.0.1:8081';
const accepted = new Counter('orders_accepted');
const reserveTime = new Trend('order_reserve_duration', true);
export const options = {
  scenarios: { sale: { executor: 'per-vu-iterations', vus: users.length, iterations: 1, maxDuration: '30s' } },
  thresholds: { checks: ['rate==1'], orders_accepted: ['count==' + config.stock], http_req_failed: ['rate==0'] },
  summaryTrendStats: ['avg','p(50)','p(95)','p(99)','max'],
};

// Issue all tokens before opening admission so all users compete for the same inventory.
export function setup() {
  const tokens = [];
  for (let i = 0; i < users.length; i += 50) {
    const batch = users.slice(i, i+50).map(auth => ({
      method: 'POST', url: base + '/voucher-order/token/' + config.voucher,
      params: { headers: {Authorization: auth}, tags: {name: 'sale_token'} }
    }));
    for (const r of http.batch(batch)) {
      if (r.status !== 200 || !r.json('success')) fail('token acquisition failed');
      tokens.push(r.json('data'));
    }
  }
  return {tokens, start: Date.now()+3000};
}

export default function(fixture) {
  const i = exec.vu.idInTest - 1;
  sleep((i % 50) * 0.02);
  http.get(base + "/health/live", {tags: {name: "warm_connection"}});
  sleep(Math.max(0, fixture.start - Date.now()) / 1000);
  const r = http.post(base + '/voucher-order/seckill/' + config.voucher, null, {
    headers: {Authorization: users[i], 'X-Seckill-Token': fixture.tokens[i]}, tags: {name: 'reserve'}
  });
  reserveTime.add(r.timings.duration);
  const result = r.status === 200 ? r.json() : {};
  const ok = result.success === true && typeof result.data === 'string';
  accepted.add(ok ? 1 : 0);
  check(r, {'queued or sold out': () => r.status === 200 && (ok || result.errorMsg === '已售罄')});
}
