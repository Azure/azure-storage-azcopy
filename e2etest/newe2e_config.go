package e2etest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
)

var GlobalConfig NewE2EConfig

// ========= Config definition ==========

/*
The env tag allows you to specify options.
The first argument will always be the environment variable name.

The following options can be as follows:
- required: The requirements of this must be fulfilled. Expected by default on the base NewE2EConfig struct.
- mutually_exclusive: Only one field under this must be completely fulfilled. Multiple is unacceptable. Ignored on values and json structs.
- minimum_required: How many required fields under this field must match, separated by an equal. Only numbers are accepted.
- default: must be the final argument, separated with an equal. Everything that follows will be used, including commas.

All immediate fields of a mutually exclusive struct will be treated as required, and all but one field will be expected to fail.
Structs that are not marked "required" will present Environment errors from "required" fields when one or more options are successfully set
*/

const (
	AzurePipeline = "AzurePipeline"

	WorkloadIdentityServicePrincipalID = "servicePrincipalId"
	WorkloadIdentityTenantID           = "tenantId"
	WorkloadIdentityToken              = "idToken"
)

type NewE2EConfig struct {
	E2EAuthConfig struct { // mutually exclusive
		SubscriptionLoginInfo struct {
			SubscriptionID string `env:"NEW_E2E_SUBSCRIPTION_ID,required"`
			DynamicOAuth   struct {
				SPNSecret struct {
					ApplicationID string `env:"NEW_E2E_APPLICATION_ID,required"`
					ClientSecret  string `env:"NEW_E2E_CLIENT_SECRET,required"`
					TenantID      string `env:"NEW_E2E_TENANT_ID"`
				} `env:",required"`
				Workload struct {
					ClientId       string `env:"servicePrincipalId,required"`
					FederatedToken string `env:"idToken,required"`
					TenantId       string `env:"tenantId,required"`
				} `env:",required"`
			} `env:",required,minimum_required=1"`
			Environment string `env:"NEW_E2E_ENVIRONMENT,required"`
		} `env:",required"`

		StaticStgAcctInfo struct {
			StaticOAuth struct {
				TenantID      string `env:"NEW_E2E_STATIC_TENANT_ID"`
				ApplicationID string `env:"NEW_E2E_STATIC_APPLICATION_ID,required"`
				ClientSecret  string `env:"NEW_E2E_STATIC_CLIENT_SECRET,required"`
			}

			// todo: should we automate this somehow? Currently each of these accounts needs some marginal boilerplate.
			Standard struct {
				AccountName string `env:"NEW_E2E_STANDARD_ACCOUNT_NAME,required"`
				AccountKey  string `env:"NEW_E2E_STANDARD_ACCOUNT_KEY,required"`
			} `env:",required"`
			HNS struct {
				AccountName string `env:"NEW_E2E_HNS_ACCOUNT_NAME,required"`
				AccountKey  string `env:"NEW_E2E_HNS_ACCOUNT_KEY,required"`
			} `env:",required"`
			PremiumPage struct {
				AccountName string `env:"NEW_E2E_PREMIUM_PAGE_ACCOUNT_NAME,required"`
				AccountKey  string `env:"NEW_E2E_PREMIUM_PAGE_ACCOUNT_KEY,required"`
			} `env:",required"`
			PremiumFileShare struct {
				AccountName string `env:"NEW_E2E_PREMIUM_FILESHARE_ACCOUNT_NAME,required"`
				AccountKey  string `env:"NEW_E2E_PREMIUM_FILESHARE_ACCOUNT_KEY,required"`
			} `env:",required"`
		} `env:",required,minimum_required=1"`
	} `env:",required,mutually_exclusive"`

	AzCopyExecutableConfig struct {
		ExecutablePath      string `env:"NEW_E2E_AZCOPY_PATH,required"`
		AutobuildExecutable bool   `env:"NEW_E2E_AUTOBUILD_AZCOPY,default=true"` // todo: make this work. It does not as of 11-21-23

		LogDropPath string `env:"AZCOPY_E2E_LOG_OUTPUT"`
	} `env:",required"`
}

func (e NewE2EConfig) StaticResources() bool {
	return e.E2EAuthConfig.SubscriptionLoginInfo.SubscriptionID == "" // all subscriptionlogininfo options would have to be filled due to required
}

// ========= Tag Definition ==========

type EnvTag struct {
	EnvName                     string
	DefaultValue                string
	Required, MutuallyExclusive bool
	MinimumRequired             uint
}

func ParseEnvTag(tag string) EnvTag {
	parts := strings.Split(tag, ",")
	var out EnvTag
	out.EnvName = parts[0]

	if len(parts) == 0 {
		return out
	}

	for i := 1; i < len(parts); i++ {
		v := parts[i]

		switch {
		case strings.EqualFold(v, "required"):
			out.Required = true
		case strings.EqualFold(v, "mutually_exclusive"):
			out.MutuallyExclusive = true
		case strings.HasPrefix(v, "default="):
			out.DefaultValue = strings.TrimPrefix(v+strings.Join(parts[i+1:], ","), "default=")
		case strings.HasPrefix(v, "minimum_required="):
			minimumReq, err := strconv.ParseUint(strings.TrimPrefix(v, "minimum_required="), 10, 32)
			if err != nil {
				panic("could not parse flag minimum_required: " + err.Error())
			}
			out.MinimumRequired = uint(minimumReq)
		}
	}

	return out
}

// ========== Config Reader Error Definition ==========

type ConfigReaderError struct {
	StructName      string
	EnvErrors       map[string]EnvError          // Mapped by env var name
	StructureErrors map[string]ConfigReaderError // Mapped by struct name (e.g. E2EAuthConfig)
	CoreError       error
}

func (c *ConfigReaderError) WrangleAsError() error {
	// Go does some weird stuff with types here.
	/*
		Error is an interface, and a pointer to ConfigReaderError satisfies that interface.
		So, when passing nil ConfigReaderError, it becomes *ConfigReaderError(<nil>),
		and is not actually nil (because the error interface interprets this as a fulfilled interface)

		Thus, we have WrangleAsError, in which we return a "real" nil that Go will understand as nil.
	*/

	if c == nil {
		return nil
	}

	return c
}

type EnvError struct {
	EnvName   string
	FieldName string
	CoreError error
}

func (e EnvError) Error(scope string) string {
	return fmt.Sprintf("%s.%s (env %s): %s", scope, e.FieldName, e.EnvName, e.CoreError.Error())
}

func NewConfigReaderErrorEnv(envName string, fieldName string, err error) *ConfigReaderError {
	return &ConfigReaderError{
		EnvErrors:       map[string]EnvError{envName: {EnvName: envName, FieldName: fieldName, CoreError: err}},
		StructureErrors: map[string]ConfigReaderError{},
	}
}

func NewConfigReaderError(StructName string) *ConfigReaderError {
	return &ConfigReaderError{
		StructName:      StructName,
		EnvErrors:       map[string]EnvError{},
		StructureErrors: map[string]ConfigReaderError{},
	}
}

func (c *ConfigReaderError) Error() string {
	return c.Flatten("")
}

func (c *ConfigReaderError) Flatten(parent string) string {
	scope := c.StructName
	if parent != "" {
		scope = parent + "." + scope
	}

	out := ""

	if c.CoreError != nil {
		out += fmt.Sprintf("%s structure error: %s\n", scope, c.CoreError.Error())
	}
	for _, v := range c.EnvErrors {
		out += v.Error(scope) + "\n"
	}

	if len(c.StructureErrors) > 0 {
		out += "\n"
	}
	for _, v := range c.StructureErrors {
		out += v.Flatten(scope) + "\n"
	}

	return out
}

func (c *ConfigReaderError) Combine(with *ConfigReaderError) {
	if with.StructName != "" && with.StructName != c.StructName {
		c.StructureErrors[with.StructName] = *with
	} else {
		for k, v := range with.EnvErrors {
			c.EnvErrors[k] = v
		}

		for k, v := range with.StructureErrors {
			c.StructureErrors[k] = v
		}

		if c.CoreError == nil { // already has nothing, will do nothing if nothing is there in the new error. This shouldn't really wind up getting used though.
			c.CoreError = with.CoreError
		}
	}
}

func (c *ConfigReaderError) Empty() bool {
	return c == nil || (len(c.StructureErrors) == 0 && len(c.EnvErrors) == 0 && c.CoreError == nil)
}

func (c *ConfigReaderError) Finalize() *ConfigReaderError {
	if c.Empty() {
		return nil
	}

	return c
}

// ========== Config Reader ===========

func SetValue(fieldName string, val reflect.Value, tag EnvTag) *ConfigReaderError {
	res, ok := os.LookupEnv(tag.EnvName)
	if !ok {
		if tag.DefaultValue != "" {
			res = tag.DefaultValue
			ok = true

			if res == "" {
				return nil // defensively code against a zero-default, though it makes no sense.
			}
		} else if tag.Required {
			return NewConfigReaderErrorEnv(tag.EnvName, fieldName, errors.New("environment variable not found"))
		} else {
			return nil // no error is needed for a unrequired field
		}
	}

	switch val.Kind() {
	case reflect.Struct, reflect.Array, reflect.Map, reflect.Slice:
		// Unmarshal onto it
		destVal := reflect.New(val.Type())
		target := destVal.Interface()
		err := json.Unmarshal([]byte(res), &target)
		if err != nil {
			return NewConfigReaderErrorEnv(tag.EnvName, fieldName, fmt.Errorf("failed to parse: %w", err))
		}

		val.Set(destVal.Elem())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		out, err := strconv.ParseInt(res, 10, 64)
		if err != nil {
			return NewConfigReaderErrorEnv(tag.EnvName, fieldName, fmt.Errorf("failed to parse: %w", err))
		}

		val.SetInt(out)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		out, err := strconv.ParseUint(res, 10, 64)
		if err != nil {
			return NewConfigReaderErrorEnv(tag.EnvName, fieldName, fmt.Errorf("failed to parse: %w", err))
		}

		val.SetUint(out)
	case reflect.Float32, reflect.Float64:
		out, err := strconv.ParseFloat(res, 64)
		if err != nil {
			return NewConfigReaderErrorEnv(tag.EnvName, fieldName, fmt.Errorf("failed to parse: %w", err))
		}

		val.SetFloat(out)
	case reflect.String:
		val.SetString(res)

	case reflect.Bool:
		if strings.EqualFold(res, "true") {
			val.SetBool(true)
		}
	}

	return nil
}

func ReadConfig(config reflect.Value, fieldName string, tag EnvTag) *ConfigReaderError {
	if tag.EnvName != "" { // This needs to be fulfilled
		return SetValue(fieldName, config, tag)
	} else if config.Kind() == reflect.Struct {
		fieldCt := config.NumField()
		sType := config.Type()

		hasRequiredFlags := false
		successfulSetCount := uint(0)

		baseError := NewConfigReaderError(fieldName)
		for i := 0; i < fieldCt; i++ {
			envTag := ParseEnvTag(sType.Field(i).Tag.Get("env"))

			if tag.MutuallyExclusive { // all semantically get treated as required for consistency.
				envTag.Required = true
			}

			if envTag.Required {
				hasRequiredFlags = true
			}

			err := ReadConfig(config.Field(i), sType.Field(i).Name, envTag)

			if err != nil && envTag.Required {
				baseError.Combine(err)
			} else if err == nil && envTag.Required {
				successfulSetCount++
			}
		}

		// The definition of "Required" is simply that all underlying conditions must satisfy.
		// If this _is_ required, it is a structure error to have an issue.
		// If it is not required, but is mutually exclusive, it is still a structure error to fulfill more than one condition.
		if tag.Required {
			if tag.MutuallyExclusive {
				// When we're mutually exclusive, everything is required, but only one must satisfy.
				if successfulSetCount > 1 {
					baseError.CoreError = errors.New("mutually exclusive struct satisfies more than one field")
					return baseError.Finalize()
				} else if successfulSetCount == 0 {
					baseError.CoreError = errors.New("mutually exclusive struct does not satisfy at least one field")
					return baseError.Finalize()
				}

				baseError = NewConfigReaderError(fieldName) // No error if only one got set
			} else if tag.MinimumRequired != 0 {
				if successfulSetCount < tag.MinimumRequired {
					baseError.CoreError = fmt.Errorf("required struct fails to fulfill at least %d conditions", tag.MinimumRequired)
					return baseError.Finalize()
				}

				baseError = NewConfigReaderError(fieldName) // No error if the required amount got set
			} else if !baseError.Empty() {
				baseError.CoreError = errors.New("required struct fails to fulfill one or more conditions")
				return baseError.Finalize()
			}
		} else {
			// when we're not required, but we see required flags, and at least one is set, all required flags under us must succeed.
			if !tag.MutuallyExclusive && hasRequiredFlags && successfulSetCount >= 1 {
				if !baseError.Empty() {
					baseError.CoreError = errors.New("required struct fails to fulfill one or more conditions")
					return baseError.Finalize()
				}
			} else if tag.MutuallyExclusive {
				// When we're mutually exclusive, everything is required, but only one must satisfy.
				if successfulSetCount > 1 {
					baseError.CoreError = errors.New("mutually exclusive struct satisfies more than one field")
					return baseError.Finalize()
				} else if successfulSetCount == 0 {
					baseError.CoreError = errors.New("mutually exclusive struct does not satisfy at least one field")
					return baseError.Finalize()
				}

				baseError = NewConfigReaderError(fieldName) // No error if only one got set
			}
		}

		return baseError.Finalize() // Assuming we're not required or mutually exclusive, and everything under us satisfies,
	} else {
		return &ConfigReaderError{
			StructName: fieldName,
			CoreError:  errors.New("struct field was not assigned an environment variable and is not traversable"),
		}
	}
}

// ========== Hook ==========

func LoadConfigHook(a Asserter) {
	a.NoError("read config", ReadConfig(reflect.ValueOf(&GlobalConfig).Elem(), "NewE2EConfig", EnvTag{Required: true}).WrangleAsError())
}

//type ConfigMissingParameters struct {
//	MissingParams []string
//}
//
//func (c ConfigMissingParameters) Error() string {
//	return fmt.Sprintf("the following config entries were missing or invalid: %s", strings.Join(c.MissingParams, ", "))
//}
//
//func (c ConfigMissingParameters) Tier() ErrorTier {
//	return ErrorTierFatal
//}
//
//func LoadConfigHook() TieredError {
//	// temp types
//	type selection struct {
//		val reflect.Value
//		tag reflect.StructTag
//	}
//	type envTag struct {
//		envName  string
//		required bool
//	}
//
//	missingRequiredVars := make([]string, 0)
//
//	// temp funcs
//	getEnv := func(tag envTag) (string, bool) {
//		out, ok := os.LookupEnv(tag.envName)
//
//		if !ok && tag.required {
//			missingRequiredVars = append(missingRequiredVars, tag.envName)
//		}
//
//		return out, ok
//	}
//	parseEnvTag := func(in string) envTag {
//		sections := strings.Split(in, ",")
//		return envTag{envName: sections[0], required: len(sections) >= 2 && strings.EqualFold(sections[1], "required")}
//	}
//
//	//reflect.ValueOf(interface{}(GlobalConfig))
//	queue := []selection{{val: reflect.ValueOf(&GlobalConfig).Elem()}}
//	for len(queue) > 0 {
//		workItem := queue[0] // pop one off the end
//		queue = queue[1:]
//
//		val := workItem.val // pull info
//		tag := workItem.tag
//
//		if envName, ok := tag.Lookup("env"); ok {
//
//		} else if val.Kind() == reflect.Struct {
//			fieldCt := val.NumField()
//			sType := val.Type()
//
//			for i := 0; i < fieldCt; i++ {
//				queue = append(queue, selection{
//					val: val.Field(i),
//					tag: sType.Field(i).Tag,
//				})
//			}
//		}
//	}
//
//	if len(missingRequiredVars) != 0 {
//		return ConfigMissingParameters{missingRequiredVars}
//	}
//
//	return nil
//}
