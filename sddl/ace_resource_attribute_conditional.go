package sddl

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Because ConditionalExpressions can be wrapped, ConditionalACEResourceAttribute serves as a top-level instance of it.
type ConditionalACEResourceAttribute struct {
	Expression ConditionalExpression
}
type ConditionalExpression struct {
	// Why a list of pointers?
	// Because when we append to an array, it gets deep copied.
	// We want pointers to the values within the parser because we attempt to divide and conquer with goroutines.
	// When we append, we have a pointer to the old copy of the array, causing a weird memory leak.
	// So, in order to test, we now have to dereference the conditional expression.
	// Perhaps we should consider dereferencing up-front here, and having a midpoint type while we're parsing.
	SubExpressions []*ConditionalExpression // referenced in Values as subExpr[x]
	Values         []string                 // The ordering and usage of these depends upon the operator specified below.
	Operator       string
	inParens       bool
}

func (c ConditionalACEResourceAttribute) StringifyResourceAttribute() string {
	return "(" + c.Expression.String() + ")"
}

func (c ConditionalExpression) String() string {
	// First of all, we need to worry about any sub expressions, and we need to stringify them first.
	stringifiedSubExpr := make([]string, len(c.SubExpressions))
	for k := range stringifiedSubExpr {
		// TODO: Emulate this recursion with a channel.
		//       It's doubtful anyone will have a conditional operator THAT deep,
		//       But if we don't fix this, it's liable to cause a dirty crash.
		//       I think this requires a serious restructure in order to PROPERLY do this.
		stringifiedSubExpr[k] = c.SubExpressions[k].String()
	}

	output := ""

	putStr := func(input string, requiresParens bool) {
		if strings.HasPrefix(input, subExprIdentifier) {
			idxStr := strings.TrimSuffix(strings.TrimPrefix(input, subExprIdentifier), "]")
			subexprInt, err := strconv.ParseInt(idxStr, 10, 64)

			if err != nil {
				panic("failed to parse integer in " + subExprIdentifier + idxStr + "]")
			}

			if requiresParens || c.SubExpressions[subexprInt].inParens {
				output += "(" + stringifiedSubExpr[subexprInt] + ")"
			} else {
				output += stringifiedSubExpr[subexprInt]
			}
		} else {
			if requiresParens {
				output += "("
			}

			output += input

			if requiresParens {
				output += ")"
			}
		}
	}

	switch c.Operator {
	case "": // No operators means this is surrounded by ().
		if len(c.Values) != 1 {
			panic("cannot surround more than one value with ()")
		}

		putStr(c.Values[0], false)
	case "==", "!=", "<=", ">=", "<", ">", "Contains", "Any_of", "&&", "||":
		if len(c.Values) != 2 {
			panic(c.Operator + " has an invalid number of values!")
		}

		putStr(c.Values[0], false)
		output += " " + c.Operator + " "
		putStr(c.Values[1], false)
	case "exists":
		if len(c.Values) != 1 {
			panic("exists has an invalid number of values!")
		}

		output += "exists "
		putStr(c.Values[0], false)
	case "Member_of":
		if len(c.Values) == 0 {
			panic("Member_of has no values!")
		}

		output += "Member_of {"
		for k, v := range c.Values {
			output += v
			if k != len(c.Values)-1 {
				output += ", "
			}
		}
		output += "}"
	case "!":
		if len(c.Values) != 1 {
			panic("! has an invalid number of values!")
		}

		output += "!"
		putStr(c.Values[0], true)
	}

	return output
}

const (
	subExprIdentifier = "subExpr["
)

var conditionalOpMap = []string{
	"==",
	"<=",
	">=",
	"!=",
	">",
	"<",
	"Contains",
	"Any_of",
	"Member_of",
	"exists",
	"!",
}

var unsafeOpWords = map[string]bool{
	"Contains":  true,
	"Any_of":    true,
	"Member_of": true,
	"exists":    true,
}

var stringEndChars = map[rune]bool{
	' ': true,
	'(': true,
	'{': true,
	'"': true,
	'<': true,
	'>': true,
	'!': true,
	'=': true,
}

func hasConditionalOpSuffix(input string) (op string, hasOp bool, isUnsafe bool) {
	for _, v := range conditionalOpMap {
		if strings.HasSuffix(input, v) {
			_, ok := unsafeOpWords[v]
			return v, true, ok
		}
	}

	return "", false, false
}

func ParseConditionalACEResourceAttribute(input string) (ConditionalACEResourceAttribute, error) {
	output := &ConditionalACEResourceAttribute{Expression: ConditionalExpression{}}

	if !strings.HasSuffix(input, ")") || !strings.HasPrefix(input, "(") {
		return *output, fmt.Errorf("resource attribute lacks surrounding parentheses")
	}

	// This is going to be mildly painful to read and understand, so documentation will be heavier than usual.
	// Go doesn't properly optimize for recursion, so if a user has a particularly nasty conditional ACE, we will stack overflow if we try to recurse.
	// So, in order to circumvent this, We'll create a queue in the form of a channel.
	// In order to prevent blocking it, we'll back it with an atomic UInt64 to denote the "planned" number of incoming parts.
	// We're going to use a number of workers and divide-and-conquer the expression until it is all evaluated.

	// conditionalPlan is an underlying struct that denotes what our worker is to parse, and where it will go.
	// Because only one worker will be doing things to a conditional expression at a time (due to the fact that channels only drop data once)
	// It's safe to just use a pointer to the conditional.
	type conditionalPlan struct {
		input string
		index int
		dest  *ConditionalExpression
	}

	// incoming and incomingAtomic are backups for when the channel empties, but there are still plans on their way.
	// This saves us from having any of our workers exiting early accidentally.
	incoming := int64(1)
	incomingAtomic := &incoming
	plans := make(chan conditionalPlan, 50)
	var conditionalWorkerWG sync.WaitGroup

	// schedule the root expression
	plans <- conditionalPlan{
		input: strings.TrimSuffix(strings.TrimPrefix(input, "("), ")"),
		index: 1, // If 1 seems odd here, it's because we removed the (
		dest:  &output.Expression,
	}

	// schedulePlan is a func designed to run as a goroutine.
	// If the underlying channel is saturated, this will prevent workers from locking up while trying to schedule something new, and allow them to get to more work.
	schedulePlan := func(plan conditionalPlan) {
		plans <- plan
	}

	// exitParsing is a func designed to fully exit the parsing loop in the case of an error state.
	// It's effectively a function-limited panic.
	ctx, cancel := context.WithCancel(context.Background())
	var (
		err     error
		errLock = &sync.Mutex{}
	)
	exitParsing := func(failReason string, index int) {
		cancel()
		errLock.Lock()
		defer errLock.Unlock()
		if err == nil {
			err = fmt.Errorf("at %d: %s", index, failReason)
		}
	}

	// conditionalParsingWorker is supposed to be fired up with as many times as runtime.GOMAXPROCS(0) allows.
	conditionalParsingWorker := func() {
		// Watch the incoming field rather than the channel itself, due to the unreliability noted above.
	retest:
		for atomic.LoadInt64(incomingAtomic) > 0 {
			var plan conditionalPlan
			var ok bool
			select {
			case <-ctx.Done(): // cleanly exit
				ok = false
			case plan, ok = <-plans: // load a plan from the channel
			case <-time.After(time.Nanosecond * 500): // We check often, but not so often that this routine basically doesn't sleep.
				goto retest // A goto is generally considered somewhat poor practice, but in this case, it's just looping around our backup incoming check.
			}

			// If the plans channel has closed or the ctx has registered done, there's nothing to do anymore.
			if !ok {
				break
			}

			if len(plan.input) == 0 {
				exitParsing("empty expression", plan.index)
				goto retest
			}

			// Start considering the structure of the segment.
			// We know for a fact that || and && denote that we are working on a multi-part expression.
			// || has lower precedence than &&
			// ! is also considered a multi-part expression.
			// () would also be considered a multi-part expression, and evaluated first.
			// Equal precedence is ordered left to right.
			// So, x == y || y != z || z != x
			// Would be evaluated as (x == y || y != z) || z != x
			// or pseudo-code: ConditionalExpression{SubExpressions: {(x==y || y != z), z != x}, Operator: "||"}
			// x == y || y != z && z != x || y != a
			// (OR) AND (OR)
			// You get the gist. The last && is king.
			// If there is no &&, the last || is king.

			// Basically, parsing this is going to be a lot of work.
			// We need to ONLY consider the symbols at the top-level scope of this expression for now, and keep a list of them.
			// As soon as we discern the IMPORTANT bits here, life will be less of a pain.
			type symbolLoc struct {
				symbol string
				loc    int
			}

			symbols := make([]symbolLoc, 0)
			scope := 0
			scopeStart := 0
			inString := false

			// For now, let's only consider "dividers".
			for k, v := range strings.Split(plan.input, "") {
				switch {
				case inString:
					if v == `"` && plan.input[k-1] != '\\' {
						inString = false
					}
				case scope > 0:
					if v == `"` {
						inString = true
					}
					// Though () isn't actually handled when searching for separators, we keep track of it anyway because
					// 1) || and && inside of () don't matter at the top level
					// 2) Later down the line, this will be handled as a subexpression
					// All we're trying to do in here is discern the existing structure.
					if v == ")" {
						scope--
					} else if v == "(" {
						scope++
					}
				case v == `"`:
					inString = true
				case v == "(":
					scopeStart = k
					scope++
				case v == ")":
					exitParsing("unexpected parentheses end", plan.index+k)
				case v == "&" || v == "|": // &&, ||
					// &/| should be followed by another &/|
					if !(k+1 >= len(plan.input)) && plan.input[k+1] == v[0] {
						break
					}

					if k-1 < 0 {
						exitParsing("unexpected lone "+v+" symbol", plan.index+k)
					}

					switch plan.input[k-1] {
					case v[0]:
						symbols = append(symbols, symbolLoc{plan.input[k-1 : k+1], k})
					default:
						exitParsing("unexpected lone "+v+" symbol", plan.index+k)
					}
				}
			}

			if scope > 0 {
				exitParsing("parentheses not closed", plan.index+scopeStart)
			}

			// Now, we should check for any conditional dividers.
			// && holds precedence over ||, so the last && (if it exists) becomes the operation here.
			// We'll go from top-down on the separators to save time.
			masterSymbol := -1 // masterSymbol indicates which one will be the "primary" symbol in this case. If masterSymbol is -1 after this, we have a lone expression.
			masterSymbolType := ""
			for i := len(symbols) - 1; i >= 0; i-- {
				if symbols[i].symbol == "&&" {
					// In this case, this is the master symbol. We can break out of the for loop.
					masterSymbol = i
					masterSymbolType = "&&"
					break
				} else if symbols[i].symbol == "||" {
					if i > masterSymbol { // This must be the furthest out to be our "master" symbol.
						masterSymbol = i
						masterSymbolType = "||"
						// We can't break out of the for loop yet, because && is a master symbol that matters more than ||.
					}
				}
			}

			if masterSymbol != -1 {
				// Target the two sub-expressions.
				plan.dest.Operator = masterSymbolType
				plan.dest.SubExpressions = append(plan.dest.SubExpressions, []*ConditionalExpression{{}, {}}...)
				plan.dest.Values = []string{subExprIdentifier + "0]", subExprIdentifier + "1]"}

				atomic.AddInt64(incomingAtomic, 2)
				go schedulePlan(conditionalPlan{
					input: plan.input[:symbols[masterSymbol].loc-1],
					index: plan.index,
					dest:  plan.dest.SubExpressions[0],
				})
				go schedulePlan(conditionalPlan{
					input: plan.input[symbols[masterSymbol].loc+1:],
					index: plan.index + symbols[masterSymbol].loc + 1,
					dest:  plan.dest.SubExpressions[1],
				})
			} else {
				// We have no more conditional splits, however something like !(condition) may still pop up.
				// Plus, as far as https://docs.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-definition-language-for-conditional-aces-#conditional-expressions
				//   goes, it seems as though (condition) == (condition) or condition == condition isn't possible (and is better done as && anyway)
				// We're going to handle this as a really weird channel of actions.
				// Why? This creates a cleaner structure to how we actually handle parsing.

				actionChannel := make(chan string, 5)
				chanClosed := false
				iterator := 0
				scope := 0
				inArray := false
				arrayStart := 0
				inString := false
				stringStart := 0
				actionChannel <- plan.input[iterator : iterator+1]
				runningString := ""

				const (
					StringEnd             = "endRunningString"
					ScheduleSubExpression = "scheduleSubExp"
					CloseActionChan       = "close"
				)

				for k := range actionChannel {
					if k == CloseActionChan {
						if !chanClosed {
							close(actionChannel)
							chanClosed = true
						}
						break // This lives outside of the switch statement to ensure the loop closes
					}

					switch k {
					case StringEnd:
						if len(actionChannel) == 0 {
							actionChannel <- CloseActionChan
						}

						if runningString == "" {
							break
						}

						// Set a parameter
						plan.dest.Values = append(plan.dest.Values, runningString)

						// || and && won't show up here
						switch plan.dest.Operator {
						case "":
							// There is no operator yet. However, we know for a fact that there is no setup preceding an operator with more than one argument.
							if len(plan.dest.Values) > 1 {
								exitParsing("invalid number of values preceding a operator", plan.index+iterator)
								actionChannel <- CloseActionChan
							}
						case "==", "!=", "<=", ">=", "<", ">", "!":
							if len(plan.dest.Values) > 2 {
								exitParsing("invalid number of values succeeding "+plan.dest.Operator, plan.index+iterator)
								actionChannel <- CloseActionChan
							}
						}

						runningString = ""

						if len(actionChannel) == 0 {
							actionChannel <- CloseActionChan
						}
					case ScheduleSubExpression: // ScheduleSubExpression is a different kind of StringEnd.
						if len(actionChannel) == 0 {
							actionChannel <- CloseActionChan
						}

						if runningString == "" {
							exitParsing("sub expression has no content", plan.index+iterator-len(runningString)+1)
						}
						// Schedule the sub expression,
						plan.dest.SubExpressions = append(plan.dest.SubExpressions, &ConditionalExpression{})

						subExprIdx := len(plan.dest.SubExpressions) - 1

						atomic.AddInt64(incomingAtomic, 1)
						schedulePlan(conditionalPlan{
							input: strings.TrimPrefix(strings.TrimSuffix(runningString, ")"), "("),
							index: plan.index + iterator - len(runningString) + 1,
							dest:  plan.dest.SubExpressions[subExprIdx],
						})

						plan.dest.SubExpressions[subExprIdx].inParens = true

						plan.dest.Values = append(plan.dest.Values, subExprIdentifier+strconv.Itoa(subExprIdx)+"]")
						runningString = ""
					default:
						switch {
						case inString: // If we're in a string, we need to override the existing parsing, and read along until the end.
							if runningString == "" {
								runningString += `"`
							}

							runningString += k
							if k == `"` {
								inString = false
								if scope == 0 {
									actionChannel <- StringEnd
								}
							}
						case inArray: // If we're in an array, we need to override the existing parsing.
							if k == "}" {
								// End the SID array. Process the last element.
								inArray = false
							}

							if (k == "}" || k == ",") || (k == " " && runningString != "") {
								if len(runningString) == 0 {
									exitParsing("invalid element in Member_of array", plan.index+iterator)
									actionChannel <- CloseActionChan
								}

								plan.dest.Values = append(plan.dest.Values, runningString)

								runningString = ""
							} else {
								if k != " " {
									runningString += k
								}
							}
						case scope > 0: // If we're in parentheses, keep reading along until we reach the end.
							if runningString == "" {
								runningString += "("
							}

							if k == `"` {
								inString = true
							}

							if k == "(" {
								scope++
							} else if k == ")" {
								scope--
							}

							runningString += k
							if scope == 0 {
								actionChannel <- ScheduleSubExpression
							}
						case k == "(": // Parentheses are starting.
							scope++
							actionChannel <- StringEnd
						case k == `"`: // A string is beginning.
							inString = true
							actionChannel <- StringEnd
							stringStart = iterator
						case k == "{": // A Member_of segment has begun.
							inArray = true
							actionChannel <- StringEnd
							arrayStart = iterator
						case k == " ": // The string stopped being continuous. Commit it.
							if runningString == "" {
								break
							}

							// So, your gut might say "but what if someone does something like ' == '?"
							// And the answer is, that's handled below in default.

							actionChannel <- StringEnd
						default:
							runningString += k

							// We handle conditional ops in the default section because you can't assign values in case statements.
							// Furthermore, this allows us to handle it in the middle of a contiguous statement (ex. "(x==y)"
							if c, ok, isUnsafe := hasConditionalOpSuffix(runningString); ok {
								// Don't accidentally put !, < or > if they're actually !=, <=, or >=
								if !((c == "!" || c == ">" || c == "<") && len(plan.input) > iterator+1 && plan.input[iterator+1] == '=') {
									nextCharEnd := false
									lastChar := iterator+1 == len(plan.input)

									if !lastChar {
										_, nextCharEnd = stringEndChars[rune(plan.input[iterator+1])]
									}

									// && and || won't show up here.
									switch c {
									case "==", "<=", ">=", "!=", "<", ">", "Contains", "Any_of":
										if len(plan.dest.Values) != 1 && (runningString == c && (lastChar || nextCharEnd)) {
											exitParsing(c+" has an invalid prior amount of values", plan.index+iterator)
											actionChannel <- CloseActionChan
										}
									case "exists", "Member_of", "!":
										if len(plan.dest.Values) > 0 && (runningString == c && (lastChar || nextCharEnd)) {
											exitParsing(c+" has an invalid prior amount of values", plan.index+iterator)
											actionChannel <- CloseActionChan
										}
									}

									if !isUnsafe || (runningString == c && (lastChar || nextCharEnd)) {
										plan.dest.Operator = c
										runningString = strings.TrimSuffix(runningString, c)
										if len(runningString) > 0 {
											actionChannel <- StringEnd
										}
									}
								}
							}
						}

						iterator++
						if iterator == len(plan.input) {
							actionChannel <- StringEnd
						} else {
							actionChannel <- plan.input[iterator : iterator+1]
						}
					}
				}

				if inArray {
					exitParsing("array never ended", arrayStart+plan.index)
				}

				if inString {
					exitParsing("string never ended", stringStart+plan.index)
				}

				switch plan.dest.Operator {
				case "":
					if len(plan.dest.Values) != 1 {
						exitParsing("incorrect argument count for a simple container expression", plan.index)
					}
				case "==", "!=", "<=", ">=", "<", ">", "Contains", "Any_of":
					if len(plan.dest.Values) != 2 {
						exitParsing("incorrect argument count for "+plan.dest.Operator, plan.index)
					}
				case "!", "exists":
					if len(plan.dest.Values) != 1 {
						exitParsing("incorrect argument count for "+plan.dest.Operator, plan.index)
					}
				case "Member_of":
					if len(plan.dest.Values) == 0 {
						exitParsing(plan.dest.Operator+" cannot have 0 arguments", plan.index)
					}
				}
			}

			if plan.dest.Operator == "" && len(plan.dest.SubExpressions) == 0 && len(plan.dest.Values) == 0 {
				exitParsing("empty expression", plan.index)
			}

			atomic.AddInt64(incomingAtomic, -1)
		}

		conditionalWorkerWG.Done()
	}

	for i := runtime.GOMAXPROCS(0); i > 0; i-- {
		conditionalWorkerWG.Add(1)
		go conditionalParsingWorker()
	}

	// Yes, a divide and conquer parser just occurred right in front of your eyes.
	conditionalWorkerWG.Wait()

	return *output, err
}
