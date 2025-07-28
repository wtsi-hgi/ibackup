package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "",
	Long:  ``,
	Run: func(_ *cobra.Command, _ []string) {
		// list := loadListToRecover()

		// sendListToServer(list, setUser, setName, prefixReplacement, isLocal)

		// // on server

		// set := getSet()
		// transformer := set.Transformer()

		// list := loadAndSortRequests(fofn, isLocal, transformer) // []struct{Local, Remote string}
		// pos := 0

		// for file := range set.Files() {
		// 	if pos > len(list) {
		// 		break
		// 	}

		// 	if isLocal {
		// 		switch strings.Compare(list[pos].Local, file) {
		// 		case 0:
		// 			pos++
		// 		case 1:
		// 			// error, file not in list
		// 		}
		// 	} else {
		// 		switch strings.Compare(list[pos].Remote, transformer(file)) {
		// 		case 0:
		// 			pos++
		// 		case 1:
		// 			// error, file not in list
		// 		}
		// 	}
		// }

		// prefixChange := setupPrefixTransformer()

		// for n := range list {
		// 	var err error

		// 	if isLocal {
		// 		list[n].Local, err = prefixChange(list[n].Local)

		// 	} else {
		// 		list[n].Local, err = prefixChange(list[n].Remote)
		// 	}

		// 	if err != nil {
		// 		return err
		// 	}
		// }

		// recover(list)
	},
}

func init() { //nolint:gochecknoinits
	RootCmd.AddCommand(restoreCmd)

	// flags specific to this sub-command
	restoreCmd.Flags().StringVarP(&setName, "name", "n", "", "a short name for this backup set")
	restoreCmd.Flags().StringVarP(&setTransformer, "transformer", "t",
		os.Getenv("IBACKUP_TRANSFORMER"), "'prefix=local:remote'")
	restoreCmd.Flags().StringVarP(&setFiles, "files", "f", "",
		"path to file with one absolute local file path per line")
	restoreCmd.Flags().StringVarP(&setDirs, "dirs", "d", "",
		"path to file with one absolute local directory path per line")
	restoreCmd.Flags().StringVarP(&setItems, "items", "i", "",
		"path to file with one absolute local directory or file path per line")
	restoreCmd.Flags().StringVarP(&setPath, "path", "p", "",
		"path to a single file or directory you wish to backup")
	restoreCmd.Flags().BoolVarP(&setNull, "null", "0", false,
		"input paths are terminated by a null character instead of a new line")
	restoreCmd.Flags().StringVar(&setUser, "user", currentUsername(),
		"pretend to be the this user (only works if you started the server)")

	restoreCmd.MarkFlagRequired("name")
}
