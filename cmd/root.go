package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/mitchellh/go-homedir"

	"github.com/mshindle/datagen/elastic"
	"github.com/mshindle/datagen/events"
	"github.com/mshindle/datagen/kafka"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type appConfig struct {
	Debug      bool
	Generators int
	Publishers int
	Kafka      kafka.Config
	Elastic    elastic.Config
	CloudLog   struct {
		Parent string
		LogID  string
	}
}

var cfgFile string
var cfgApp appConfig

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "datagen",
	Short: "generate mock data and publish it to an endpoint",
	Long:  ``,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
		zerolog.SetGlobalLevel(zerolog.InfoLevel)

		err := viper.Unmarshal(&cfgApp)
		if err != nil {
			return err
		}

		if cfgApp.Debug {
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		}
		return nil
	},
	SilenceErrors: true,
	SilenceUsage:  true,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("exiting application...")
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.govatar.yaml)")
	rootCmd.PersistentFlags().Bool("debug", false, "sets log level to debug")
	rootCmd.PersistentFlags().IntP("generators", "g", 1, "set the number of generators")
	rootCmd.PersistentFlags().IntP("publishers", "p", 1, "set the number of publishers")

	_ = viper.BindPFlag("debug", rootCmd.PersistentFlags().Lookup("debug"))
	_ = viper.BindPFlag("generators", rootCmd.PersistentFlags().Lookup("generators"))
	_ = viper.BindPFlag("publishers", rootCmd.PersistentFlags().Lookup("publishers"))

	viper.SetDefault("generators", 1)
	viper.SetDefault("publishers", 1)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			log.Fatal().Err(err).Msg("exiting application...")
		}

		// Search config in home directory with name ".govatar" (without extension).
		viper.AddConfigPath(".")
		viper.AddConfigPath(home)
		viper.SetConfigType("yml")
		viper.SetConfigName(".datagen")
	}

	// read in environment variables that match
	viper.AutomaticEnv()

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info().Str("file", viper.ConfigFileUsed()).Msg("using config file")
	}
}

// signalEngine executes the running of the engine and wrapping it around an
// os.Signal so the process can be killed cleanly from the cmdline
func signalEngine(engine *events.Engine) error {
	done, _ := engine.Run()
	defer close(done)

	// Wait for interrupt signal to gracefully shutdown the server with
	// a timeout of 5 seconds.
	quit := make(chan os.Signal)

	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be caught, so don't need to add it
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("shutting down generation")

	// tell the engine to stop....
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//defer cancel()

	return nil
}

func generateMobileLog() events.Event {
	e, err := events.MockMobileLog()
	if err != nil {
		log.Error().Err(err).Msg("unable to generate fake data")
		return nil
	}
	return e
}

func shootCraps() events.Generator {
	c := events.NewCup(6, 2)
	return events.GeneratorFunc(func() events.Event {
		return c.Throw()
	})
}
