package main

import (
	"bufio"
	"io"
	"log"
	"os"

	cli "gopkg.in/urfave/cli.v1"

	"github.com/transactional-cloud-serving-benchmark/tcsb/simulation"
)

//var (
//	scenario = flag.String("scenario", "", "Choose from: keyvalue")
//	stage = flag.String("stage", "", "Choose from: prepare, exec")
//	params = flag.String("params", "", "k=v,k=v formatted parameters for the particular scenario")
//)

func main() {
	//flag.Parse()
	//app := cli.NewApp()
	//app.Name = "generate_commands"
	//app.Usage = "Generate TCSB commands to execute later, for a given scenario and database type."
	//app.Flags = []cli.Flag{
	//	cli.StringFlag{
	//		Name:  "lang",
	//		Value: "english",
	//		Usage: "language for the greeting",
	//	},
	//}
	//app.Commands = []cli.Command{
	//	{
	//		Name:    "schema",
	//		Aliases: []string{},
	//		Usage:   "Generate schema creation commands.",
	//		Subcommands: []cli.Command{
	//			{
	//				Name:  "keyvalue",
	//				Usage: "The key-value scenario.",
	//				Subcommands: []cli.Command{
	//					{
	//						Name:    "example_server",
	//						Aliases: []string{},
	//						Usage:   "Use k",
	//						Action: func(c *cli.Context) error {
	//							//fmt.Println("new task template: ", c.Args().First())
	//							return nil
	//						},
	//					},
	//				},
	//			},
	//		},
	//	},
	//}
	app := cli.NewApp()

	app.Action = func(c *cli.Context) error {
		if c.NArg() != 5 {
			log.Printf("usage: %s <scenario> <database_kind> <command_mode> <emit_mode> <params>", c.App.Name)
			log.Printf("  scenario:      [keyvalue]")
			log.Printf("  database_kind: [gold_memory]")
			log.Printf("  command_mode:  [schema, exec]")
			log.Printf("  emit_mode:     [binary, debug]")
			log.Printf("  params:        k=v,k=v,k=v (as needed)")
			log.Fatalf("exiting with error")
		}

		scenario_name := c.Args().Get(0)
		database_kind_name := c.Args().Get(1)
		command_mode_name := c.Args().Get(2)
		emit_mode_name := c.Args().Get(3)
		params_string := c.Args().Get(4)

		scenario, err := simulation.GetScenarioByName(scenario_name)
		if err != nil {
			log.Fatal(err)
		}

		if err := scenario.SetDatabaseKindByName(database_kind_name); err != nil {
			log.Fatal(err)
		}

		err = scenario.SetCommandModeByName(command_mode_name)
		if err != nil {
			log.Fatal(err)
		}

		err = scenario.SetEmitModeByName(emit_mode_name)
		if err != nil {
			log.Fatal(err)
		}

		err = scenario.SetParamsFromString(params_string)
		if err != nil {
			log.Fatal(err)
		}


		out := bufio.NewWriter(os.Stdout)
		defer out.Flush()

		emitter := scenario.NewEmitter(out)
		for {
			err := emitter()
			if err == nil {
				continue
			}

			if err == io.EOF {
				break
			}

			log.Fatalf("command generation failed: %s", err.Error())
		}

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
