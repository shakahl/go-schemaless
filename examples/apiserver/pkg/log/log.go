package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New is a fuzzy blanket over our underlying zap logging configuration.
// This may change over time.
func New(useJSON bool) (*zap.Logger, error) {
	// See go.uber.org/zap/example_test.go func
	// Example_advancedConfiguration for more context.

	// First, define our level-handling logic.
	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel
	})

	// High-priority output should also go to standard error, and low-priority
	// output should also go to standard out.
	consoleDebugging := zapcore.Lock(os.Stdout)

	consoleErrors := zapcore.Lock(os.Stderr)

	var encoder zapcore.Encoder
	encoderCfg := zap.NewDevelopmentEncoderConfig()
	if useJSON {

		encoderCfg.CallerKey = "c"
		encoderCfg.TimeKey = "t"
		encoderCfg.LevelKey = "l"
		encoderCfg.MessageKey = "m"

		encoder = zapcore.NewJSONEncoder(encoderCfg)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	}

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, consoleErrors, highPriority),
		zapcore.NewCore(encoder, consoleDebugging, lowPriority),
	)

	// With AddCaller(), we attach (file/line) for every logging statement.
	// zap.AddStacktrace( highPriority ) uses our error-level enabler
	// declared above to conditionally add stacktraces.
	l := zap.New(core, zap.AddCaller(), zap.AddStacktrace(highPriority))

	// Override the global logger for the 'log' package
	zap.RedirectStdLog(l)

	return l, nil
}
