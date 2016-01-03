# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-out_solr"
  spec.version       = "0.1.0"
  spec.authors       = ["Minoru Osuka"]
  spec.email         = ["minoru.osuka@gmail.com"]

  spec.summary       = %q{Solr output plugin for Fluent event collector}
  spec.description   = spec.summary
  spec.homepage      = "https://github.com/mosuka/fluent-plugin-out_solr"

  spec.license       = "Apache-2.0"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "fluentd"
  spec.add_runtime_dependency "rsolr"
  spec.add_runtime_dependency "zk"
  spec.add_runtime_dependency "rsolr-cloud"

  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency 'test-unit', '~> 3.1.0'
  spec.add_development_dependency 'minitest', '~> 5.8'
end
