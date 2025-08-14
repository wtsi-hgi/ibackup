/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package transfer

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/wtsi-hgi/ibackup/errs"
)

// PathTransformer is a function that given a local path, returns the
// corresponding remote path.
type PathTransformer func(local string) (remote string, err error)

// TransformerInfo contains metadata about a transformer along with the
// transformer itself.
type TransformerInfo struct {
	Name        string
	Description string
	Transformer PathTransformer
}

// PrefixTransformer returns a PathTransformer that will replace localPrefix
// in any path given to it with remotePrefix, and return the result.
//
// If the given path does not start with localPrefix, returns the path prefixed
// with remotePrefix (treating the given path as relative to localPrefix).
func PrefixTransformer(localPrefix, remotePrefix string) PathTransformer {
	return func(local string) (string, error) {
		return filepath.Join(remotePrefix, strings.TrimPrefix(local, localPrefix)), nil
	}
}

// RegexTransformer can make a PathTransformer based on regular expressions.
type RegexTransformer struct {
	Name        string
	Description string
	Match       *regexp.Regexp
	Replace     string
}

// PathTransformer returns a PathTransformer that applies our regex
// transformation to given paths.
func (rt *RegexTransformer) PathTransformer() PathTransformer {
	return func(local string) (string, error) {
		local, err := filepath.Abs(local)
		if err != nil {
			return "", err
		}

		if !rt.Match.MatchString(local) {
			return "", errs.PathError{Msg: fmt.Sprintf("not a valid %s path", rt.Name), Path: local}
		}

		return rt.Match.ReplaceAllString(local, rt.Replace), nil
	}
}

// ToInfo converts the RegexTransformer to a TransformerInfo.
func (rt *RegexTransformer) ToInfo() TransformerInfo {
	return TransformerInfo{
		Name:        rt.Name,
		Description: rt.Description,
		Transformer: rt.PathTransformer(),
	}
}
