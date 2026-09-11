import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

const jobsCompleted = new Counter("jobs_completed");
const completionDuration = new Trend(
  "job_completion_duration",
  true,
);

export const options = {
  vus: 10,
  duration: "30s",

  thresholds: {
    http_req_failed: ["rate<0.01"],
    checks: ["rate>0.99"],
    job_completion_duration: ["p(95)<5000"],
  },
};

const baseURL =
  __ENV.BASE_URL || "http://host.docker.internal:8080";

export default function () {
  const startedAt = Date.now();

  const createResponse = http.post(
    `${baseURL}/jobs`,
    JSON.stringify({
      type: "generate_report",
      payload: {
        report_name: `load-${__VU}-${__ITER}-${Date.now()}`,
      },
      max_attempts: 3,
      timeout_seconds: 30,
    }),
    {
      headers: {
        "Content-Type": "application/json",
      },
    },
  );

  const created = check(createResponse, {
    "job accepted": (response) => response.status === 202,
  });

  if (!created) {
    sleep(0.1);
    return;
  }

  const jobID = createResponse.json("id");

  for (let poll = 0; poll < 150; poll++) {
    sleep(0.1);

    const statusResponse = http.get(
      `${baseURL}/jobs/${jobID}`,
    );

    if (statusResponse.status !== 200) {
      continue;
    }

    const status = statusResponse.json("status");

    if (status === "completed") {
      jobsCompleted.add(1);

      completionDuration.add(
        Date.now() - startedAt,
      );

      check(statusResponse, {
        "job completed": () => true,
      });

      return;
    }

    if (status === "failed") {
      check(statusResponse, {
        "job completed": () => false,
      });

      return;
    }
  }

  check(null, {
    "job completed": () => false,
  });
}