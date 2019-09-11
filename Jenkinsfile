builder(
        jUnitReportsPath: 'junit-reports',
        coverageReportsPath: 'coverage-reports',
        slackTargetNames: '#radio-jticker',
        slackNotificationBranchFilter: '^(master|develop|(release|feature|bugfix)/[a-zA-z0-9.-]*)$',
        buildTasks: [
                [
                        name: "Linters",
                        type: "lint",
                        method: "inside",
                        runAsUser: "root",
                        entrypoint: "",
                        jUnitPath: '/junit-reports',
                        command: [
                                'pip install -e ./[dev]',
                                'mkdir -p /junit-reports',
                                'pylama',
                                'mypy --junit-xml=/junit-reports/mypy-junit-report.xml .',
                        ],
                ],
                [
                        name: 'Tests',
                        type: 'test',
                        method: 'inside',
                        runAsUser: 'root',
                        entrypoint: '',
                        jUnitPath: '/junit-reports',
                        coveragePath: '/coverage-reports',
                        command: [
                                'pip install -e ./[dev]',
                                'mkdir -p /junit-reports',
                                'pytest --junitxml=/junit-reports/pytest-junit-report.xml --cov-report xml:/coverage-reports/pytest-coverage-report.xml',
                        ],
                ],
        ],
)
