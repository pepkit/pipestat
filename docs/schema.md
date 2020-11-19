# Schema specification

One of the *required* pipestat inputs is a schema file. This is the central piece of information that pipestat uses to operate.

**Schema is the source of results types specification that can be reported with pipestat.**

Pipestat uses the input schema as a base for creating a collection of self-contained result-specific [jsonschema schemas](https://json-schema.org/) that are used to **validate** the reported results prior to inserting into the database or saving in the YAML results file, depending on the selected backend.

## Components

Each schema is a YAML-formatted file composed of a set of self-contained result definitions. The top level keys are the unique result identifiers. The result definitions are jsonschema schemas. For a minimal schema, only `type` attribute is required. `type` keyword indicates the required type of the result to be reported. Please refer to the jsonschema documentation to learn more about the types and other attributes. This is an example of such component:

```yaml
result_identifier:
  type: <type>
``` 

Importantly, pipestat extends jsonschema vocabulary by adding two additional types: `image` and `file`. These types require reporting objects with the following attributes:

- `file`: 
    - `path`: path to the reported file
    - `title`: human readable description of the file
- `image`: 
    - `path`: path to the reported image, usually PDF
    - `thumbnail`: path to the reported thumbnail, usually PNG or JPEG
    - `title`: human readable description of the image    

Therefore, in practice, a result of type `file` is equivalent to:

```yaml
type: object
properties:
    path:
      type: string
    title:
      type: string
``` 

## Basic example

Here's a simple schema example that showcases most of the supported types in a basic for:


```yaml
number_of_things:
  type: integer
  description: "Number of things"
percentage_of_things:
  type: number
  description: "Percentage of things"
name_of_something:
  type: string
  description: "Name of something"
swtich_value:
  type: boolean
  description: "Is the switch on of off"
collection_of_things:
  type: array
  description: "This store collection of values"
output_object:
  type: object
  description: "Object output"
output_file:
  type: file
  description: "This a path to the output file"
output_image:
  type: image
  description: "This a path to the output image"
``` 

## More complex example

Here's a more complex schema example that showcases some of the more advanced jsonschema features:

```yaml
number_of_things:
  type: integer
  description: "Number of things, min 20, multiple of 10"
  multipleOf: 10
  minimum: 20
name_of_something:
  type: string
  description: "Name of something, min len 2 characters"
  minLength: 2
collection_of_things:
  type: array
  items:
    type: string
  description: "This store collection of strings"
output_object:
  type: object
  properties:
    property1:
      array:
        items: 
          type: integer
    property2:
      type: boolean
  required:
    - property1
  description: "Object output with required array of integers and optional boolean"
``` 
