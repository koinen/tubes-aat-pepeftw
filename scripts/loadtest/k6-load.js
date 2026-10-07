import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";
import { bearer } from "./token.js";

const API = __ENV.API_URL || "http://client-api:8080";
const rateLimited = new Counter("rate_limited_429");

http.setResponseCallback(http.expectedStatuses(200, 429));

export const options = {
	scenarios: {
		sustained: { executor: "constant-vus", vus: 50, duration: "60s" },
	},
	thresholds: {
		http_req_failed: ["rate<0.01"],
	},
	summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
};

export default function () {
	const res = http.get(`${API}/hazards?limit=50`, {
		headers: { Authorization: `Bearer ${bearer()}` },
		tags: { name: "hazards" },
	});
	if (res.status === 429) {
		rateLimited.add(1);
	}
	check(res, { "served or rate limited": (r) => r.status === 200 || r.status === 429 });
}
