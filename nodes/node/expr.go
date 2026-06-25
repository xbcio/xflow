package node

// buildExprEnv constructs the expression evaluation environment from node input.
// Available variables: $input (Data), $inputs (multi-port), $vars, $config, $params.
func buildExprEnv(input *Input) map[string]any {
	env := make(map[string]any, 16)

	if input.Data != nil {
		for k, v := range input.Data {
			env[k] = v
		}
	}

	env["$input"] = input.Data
	env["$inputs"] = input.Inputs
	env["$vars"] = input.Vars
	env["$config"] = input.Config
	env["$params"] = input.Params

	return env
}
