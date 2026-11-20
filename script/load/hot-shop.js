import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: Number(__ENV.VUS || 50),
  duration: __ENV.DURATION || '15s',
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'] },
};

const id = Number(__ENV.SHOP_ID || 2);
const url = (__ENV.BASE_URL || 'http://127.0.0.1:8081') + '/shop/' + id;
export default function () {
  const response = http.get(url, { tags: { name: 'shop_detail' } });
  check(response, { 'expected shop': r => r.status === 200 && r.json('data.id') === id });
}
