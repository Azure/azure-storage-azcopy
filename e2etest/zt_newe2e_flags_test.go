package e2etest

import "flag"

var runInteractiveTest = flag.Bool("run-interactive-test", false, "Whether or not to run interactive tests (e.g. browser, device code). These must be run manually due to interactive nature.")
