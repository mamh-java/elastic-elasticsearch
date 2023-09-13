import { parse, stringify } from "yaml";
import { readFileSync, readdirSync } from "fs";
import { basename } from "path";
import { execSync } from "child_process";

// TODO remove these before PR
process.env["GITHUB_PR_LABELS"] = process.env["GITHUB_PR_LABELS"] || "release_note:skip,:Delivery/Packaging,v8.11.0";
process.env["GITHUB_PR_TARGET_BRANCH"] = process.env["GITHUB_PR_TARGET_BRANCH"] || "main";
// process.env["GITHUB_PR_TRIGGER_COMMENT"] =
//   "hey run elasticsearch-ci/build-benchmarks please and run elasticsearch-ci/part-2";

const BWC_VERSIONS = parse(readFileSync(".ci/bwcVersions", "utf-8")).BWC_VERSION;
const SNAPSHOT_BWC_VERSIONS = parse(readFileSync(".ci/snapshotBwcVersions", "utf-8")).BWC_VERSION;

type PipelineConfig = {
  config?: {
    "allow-labels"?: string | string[];
    "skip-labels"?: string | string[];
    "included-regions"?: string | string[];
    "excluded-regions"?: string | string[];
    "trigger-phrase"?: string;
  };
};

type BuildkiteStep = {
  steps?: BuildkiteStep[];
  group?: string;
  bwc_template?: boolean;
};

type BuildkitePipeline = {
  steps?: BuildkiteStep[];
};

type Pipeline = PipelineConfig &
  BuildkitePipeline & {
    name?: string;
  };

let defaults: PipelineConfig = { config: {} };
defaults = parse(readFileSync(".buildkite/pipelines/pull-request/.defaults.yml", "utf-8"));
defaults.config = defaults.config || {};

let pipelines: Pipeline[] = [];
const files = readdirSync(".buildkite/pipelines/pull-request");
for (const file of files) {
  if (!file.endsWith(".yml") || file.endsWith(".defaults.yml")) {
    continue;
  }

  let yaml = readFileSync(`.buildkite/pipelines/pull-request/${file}`, "utf-8");
  yaml = yaml.replaceAll("$SNAPSHOT_BWC_VERSIONS", JSON.stringify(SNAPSHOT_BWC_VERSIONS));
  const pipeline: Pipeline = parse(yaml) || {};

  pipeline.config = { ...defaults.config, ...(pipeline.config || {}) };

  // '.../build-benchmark.yml' => 'build-benchmark'
  const name = basename(file).split(".", 2)[0];
  pipeline.name = name;
  pipeline.config["trigger-phrase"] = pipeline.config["trigger-phrase"] || `.*run\\W+elasticsearch-ci/${name}.*`;

  pipelines.push(pipeline);
}

const labels = (process.env["GITHUB_PR_LABELS"] || "")
  .split(",")
  .map((x) => x.trim())
  .filter((x) => x);

const mergeBase = execSync(`git merge-base ${process.env["GITHUB_PR_TARGET_BRANCH"]} HEAD`).toString().trim();

const changedFilesOutput = execSync(`git diff --name-only ${mergeBase}`).toString().trim();

const changedFiles = changedFilesOutput
  .split("\n")
  .map((x) => x.trim())
  .filter((x) => x);

const getArray = (strOrArray: string | string[] | undefined): string[] => {
  if (typeof strOrArray === "undefined") {
    return [];
  }

  return typeof strOrArray === "string" ? [strOrArray] : strOrArray;
};

const labelCheckAllow = (pipeline: Pipeline): boolean => {
  if (pipeline.config?.["allow-labels"]) {
    return getArray(pipeline.config["allow-labels"]).some((label) => labels.includes(label));
  }
  return true;
};

const labelCheckSkip = (pipeline: Pipeline): boolean => {
  if (pipeline.config?.["skip-labels"]) {
    return !getArray(pipeline.config["skip-labels"]).some((label) => labels.includes(label));
  }
  return true;
};

// Exclude the pipeline if all of the changed files in the PR are in at least one excluded region
const changedFilesExcludedCheck = (pipeline: Pipeline): boolean => {
  if (pipeline.config?.["excluded-regions"]) {
    return !changedFiles.every((file) =>
      getArray(pipeline.config?.["excluded-regions"]).some((region) => file.match(region))
    );
  }
  return true;
};

// Include the pipeline if all of the changed files in the PR are in at least one included region
const changedFilesIncludedCheck = (pipeline: Pipeline): boolean => {
  if (pipeline.config?.["included-regions"]) {
    return changedFiles.every((file) =>
      getArray(pipeline.config?.["included-regions"]).some((region) => file.match(region))
    );
  }
  return true;
};

const triggerCommentCheck = (pipeline: Pipeline): boolean => {
  if (process.env["GITHUB_PR_TRIGGER_COMMENT"] && pipeline.config?.["trigger-phrase"]) {
    return !!process.env["GITHUB_PR_TRIGGER_COMMENT"].match(pipeline.config["trigger-phrase"]);
  }
  return false;
};

let filters: ((pipeline: Pipeline) => boolean)[] = [
  labelCheckAllow,
  labelCheckSkip,
  changedFilesExcludedCheck,
  changedFilesIncludedCheck,
];

// When triggering via comment, we ONLY want to run pipelines that match the trigger phrase, regardless of labels, etc
if (process.env["GITHUB_PR_TRIGGER_COMMENT"]) {
  filters = [triggerCommentCheck];
}

for (const filter of filters) {
  pipelines = pipelines.filter(filter);
}

// Recursively check for any steps that have a bwc_template attribute and expand them out into multiple steps, one for each BWC_VERSION
const doBwcTransforms = (step: BuildkitePipeline | BuildkiteStep) => {
  const stepsToExpand = (step.steps || []).filter((s) => s.bwc_template);
  step.steps = (step.steps || []).filter((s) => !s.bwc_template);

  for (const s of step.steps) {
    if (s.steps?.length) {
      doBwcTransforms(s);
    }
  }

  for (const stepToExpand of stepsToExpand) {
    for (const bwcVersion of BWC_VERSIONS) {
      const newStepJson = JSON.stringify(stepToExpand).replaceAll("$BWC_VERSION", bwcVersion);
      const newStep = JSON.parse(newStepJson);
      delete newStep.bwc_template;
      step.steps.push(newStep);
    }
  }
};

for (const pipeline of pipelines) {
  doBwcTransforms(pipeline);
}

pipelines.sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));

for (const pipeline of pipelines) {
  const finalPipeline = { ...pipeline };
  delete finalPipeline.config;
  delete finalPipeline.name;

  if (!process.env.CI) {
    // Just for local debugging purposes
    console.log("");
    console.log(stringify(finalPipeline));
  } else {
    execSync(`buildkite-agent pipeline upload`, {
      input: stringify(finalPipeline),
      stdio: ["pipe", "inherit", "inherit"],
    });
  }
}
