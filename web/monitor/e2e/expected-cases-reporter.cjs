class ExpectedCasesReporter {
  constructor(options = {}) {
    if (!Number.isSafeInteger(options.expectedCases) || options.expectedCases <= 0) {
      throw new Error('expectedCases must be a positive integer');
    }

    this.expectedCases = options.expectedCases;
    this.collectedCases = 0;
    this.executedCases = 0;
    this.configurationProblems = [];
  }

  onBegin(config, suite) {
    this.collectedCases = suite.allTests().length;

    if (config.workers !== 1) {
      this.configurationProblems.push(`expected one worker, received ${config.workers}`);
    }

    for (const project of config.projects) {
      if (project.retries !== 0) {
        this.configurationProblems.push(
          `expected zero retries for project ${project.name}, received ${project.retries}`,
        );
      }
    }
  }

  onTestEnd(_test, result) {
    if (result.status !== 'skipped') {
      this.executedCases += 1;
    }
  }

  onEnd() {
    const problems = [...this.configurationProblems];

    if (this.collectedCases !== this.expectedCases) {
      problems.push(
        `expected ${this.expectedCases} mocked cases, collected ${this.collectedCases}`,
      );
    }

    if (this.executedCases !== this.expectedCases) {
      problems.push(
        `expected ${this.expectedCases} mocked cases to execute, observed ${this.executedCases}`,
      );
    }

    if (problems.length > 0) {
      for (const problem of problems) {
        console.error(`[mocked-browser-count] ${problem}`);
      }
      return { status: 'failed' };
    }

    console.log(`[mocked-browser-count] executed ${this.executedCases} expected mocked cases`);
  }
}

module.exports = ExpectedCasesReporter;
