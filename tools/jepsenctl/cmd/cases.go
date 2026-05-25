package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

type jepsenCase struct {
	Slug        string
	Spec        string
	Description string
}

type caseTargetValidation struct {
	CaseCount    int
	TargetCount  int
	MissingLines []string
}

type casesValidateOptions struct {
	casesFile string
	listCases string
	makefiles []string
}

func newCasesCommand(stdout, stderr io.Writer) *cobra.Command {
	cases := &cobra.Command{
		Use:   "cases",
		Short: "work with Jepsen case registry",
	}

	cases.AddCommand(newCasesValidateCommand(stdout, stderr))

	return cases
}

func newCasesValidateCommand(stdout, stderr io.Writer) *cobra.Command {
	options := casesValidateOptions{
		listCases: "./jepsen/bin/list-cases",
	}

	validate := &cobra.Command{
		Use:   "validate",
		Short: "validate Jepsen case registry targets",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("cases validate does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			caseOutput, err := loadCaseRegistry(options)
			if err != nil {
				return err
			}

			cases, err := parseListCasesOutput(caseOutput)
			if err != nil {
				return err
			}

			makefiles := options.makefiles
			if len(makefiles) == 0 {
				makefiles, err = discoverMakefiles(".")
				if err != nil {
					return err
				}
			}

			targets, err := parseMakeTargets(makefiles)
			if err != nil {
				return err
			}

			result := validateCaseTargets(cases, targets)
			for _, line := range result.MissingLines {
				fmt.Fprintln(stderr, line)
			}
			if len(result.MissingLines) > 0 {
				return fmt.Errorf("missing %d Jepsen case Make target(s)", len(result.MissingLines))
			}

			fmt.Fprintf(stdout, "validated %d Jepsen cases and %d required Make targets\n", result.CaseCount, result.TargetCount)
			return nil
		},
	}

	validate.Flags().StringVar(&options.casesFile, "cases-file", "", "path to captured jepsen/bin/list-cases output")
	validate.Flags().StringVar(&options.listCases, "list-cases", options.listCases, "path to jepsen/bin/list-cases")
	validate.Flags().StringArrayVar(&options.makefiles, "makefile", nil, "Makefile path to scan for Jepsen case targets; may be repeated")

	return validate
}

func loadCaseRegistry(options casesValidateOptions) ([]byte, error) {
	if options.casesFile != "" {
		return os.ReadFile(options.casesFile)
	}

	cmd := exec.Command(options.listCases)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", options.listCases, err)
	}
	return output, nil
}

func parseListCasesOutput(output []byte) ([]jepsenCase, error) {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	var cases []jepsenCase
	seen := make(map[string]struct{})

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("parse list-cases line %d: expected slug and workload:nemesis", lineNumber)
		}

		slug := fields[0]
		spec := fields[1]
		if !strings.Contains(spec, ":") {
			return nil, fmt.Errorf("parse list-cases line %d: spec %q must use workload:nemesis format", lineNumber, spec)
		}
		if _, ok := seen[slug]; ok {
			return nil, fmt.Errorf("parse list-cases line %d: duplicate case slug %q", lineNumber, slug)
		}
		seen[slug] = struct{}{}

		cases = append(cases, jepsenCase{
			Slug:        slug,
			Spec:        spec,
			Description: strings.Join(fields[2:], " "),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan list-cases output: %w", err)
	}
	if len(cases) == 0 {
		return nil, fmt.Errorf("list-cases output did not contain any cases")
	}

	return cases, nil
}

func discoverMakefiles(root string) ([]string, error) {
	makefiles := []string{filepath.Join(root, "Makefile")}

	matches, err := filepath.Glob(filepath.Join(root, "mk", "*.mk"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	makefiles = append(makefiles, matches...)

	return makefiles, nil
}

func parseMakeTargets(paths []string) (map[string]struct{}, error) {
	targets := make(map[string]struct{})

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read makefile %s: %w", path, err)
		}

		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" || strings.HasPrefix(line, "\t") || strings.HasPrefix(line, " ") {
				continue
			}

			colonIndex := strings.IndexByte(line, ':')
			if colonIndex < 0 {
				continue
			}

			assignmentIndex := strings.IndexByte(line, '=')
			if assignmentIndex >= 0 && assignmentIndex < colonIndex {
				continue
			}

			left := strings.TrimSpace(line[:colonIndex])
			if left == "" || strings.HasPrefix(left, ".") || strings.Contains(left, "$") {
				continue
			}

			for _, target := range strings.Fields(left) {
				targets[target] = struct{}{}
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("scan makefile %s: %w", path, err)
		}
	}

	return targets, nil
}

func validateCaseTargets(cases []jepsenCase, targets map[string]struct{}) caseTargetValidation {
	var missing []string

	for _, testCase := range cases {
		for _, target := range []string{
			"jepsen-case-" + testCase.Slug,
			"jepsen-docker-case-" + testCase.Slug,
		} {
			if _, ok := targets[target]; !ok {
				missing = append(missing, fmt.Sprintf("missing Make target for Jepsen case %s: %s", testCase.Slug, target))
			}
		}
	}

	return caseTargetValidation{
		CaseCount:    len(cases),
		TargetCount:  len(cases) * 2,
		MissingLines: missing,
	}
}
