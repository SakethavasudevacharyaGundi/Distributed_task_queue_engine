import http from 'k6/http';
import { check } from 'k6';

export const options = {
    vus: 10,
    duration: '15s',
};

export default function () {

    const payload = JSON.stringify({
        name: "send_email",
        payload: "hello",
        priority: 1,
        max_retries: 3,
    });

    const res = http.post(
        'http://127.0.0.1:8081/tasks',
        payload,
        {
            headers: {
                'Content-Type': 'application/json',
            },
        }
    );

    check(res, {
        'status is success': (r) =>
            r.status >= 200 && r.status < 300,
    });
}