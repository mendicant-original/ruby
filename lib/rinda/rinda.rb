require 'drb/drb'
require 'thread'

##
# == Overview
#
#
# Rinda is a ruby implementation of the Linda distributed computing paradigm.
#
# Linda is a model of coordination and communication among several parallel
# processes operating upon objects stored in and
# retrieved from shared, virtual, associative memory.
#
# In Rinda, distributed objects are stored in a TupleSpace in the form
# of Tuples, ie:
#
#  ['people', 'developers', John']
#  ['people', 'developers', Marcus']
#
# A TupleSpace is an implementation of the associative memory paradigm
# for parallel/distributed computing. It provides a repository of Tuples
# that can be accessed concurrently.
#
# As an illustrative example, consider that there are a group of
# processors that produce pieces of data and a group of processors
# that use the data. Producers post their data as Tuples in the
# TupleSpace, and consumers then retrieve data from the TupleSpace
# that match a certain pattern. This is also known as the blackboard metaphor.
#
# Rinda is built on the top of DRb.
#
# == Code examples
#
# Here is an example on how to build a distributed system using Rinda.
#
# The system is composed of three ruby applications
#
# * client.rb
# * server.rb
# * tuple_space.rb
#
# === tuple_space.rb
#
# It serves the TupleSpace that is going to be used by the two others.
#
#   require 'drb/drb'
#   require 'rinda/tuplespace'
#
#   uri = druby://localhost:8787
#
#   DRb.start_service(uri, Rinda::TupleSpace.new)
#   puts DRb.uri
#   DRb.thread.join
#
# === client.rb
#
# It produces 10 tuples. Those tuples are manipulated in server.rb
# Then it takes from TupleSpace 10 tuples and show the results.
#
#   require 'drb/drb'
#   require 'rinda/rinda'
#
#   uri = druby://localhost:8787
#
#   DRb.start_service
#   tuple_space = Rinda::TupleSpaceProxy.new(DRbObject.new(nil, uri))
#
#   (1..10).each do |number|
#     tuple_space.write(['sum', DRb.uri, number])
#   end
#
#   (1..10).each do |number|
#     ans = tuple_space.take(['ans', DRb.uri, number, nil])
#     p [ans[2], ans[3]]
#   end
#
# === server.rb
#
# It consumes all tuples in the form ['sum', *, *]
# and for each one that it consumes it produces a tuple
# with the result of the manipulation.
#
#   require 'drb/drb'
#   require 'rinda/rinda'
#
#   def do_it(value)
#     puts "do_it(#{value})"
#     value + value
#   end
#
#   uri = druby://localhost:8787
#
#   DRb.start_service
#   tuple_space = Rinda::TupleSpaceProxy.new(DRbObject.new(nil, uri))
#
#   while true
#     r = tuple_space.take(['sum', nil, nil])
#     v = do_it(r[2])
#     tuple_space.write(['ans', r[1], r[2], v])
#   end

module Rinda

  ##
  # Rinda error base class

  class RindaError < RuntimeError; end

  ##
  # Raised when a hash-based tuple has an invalid key.

  class InvalidHashTupleKey < RindaError; end

  ##
  # Raised when trying to use a canceled tuple.

  class RequestCanceledError < ThreadError; end

  ##
  # Raised when trying to use an expired tuple.

  class RequestExpiredError < ThreadError; end

  ##
  # A tuple is the elementary object in Rinda programming.
  # Tuples may be matched against templates if the tuple and
  # the template are the same size.

  class Tuple

    ##
    # Creates a new Tuple from +ary_or_hash+ which must be an Array or Hash.

    def initialize(ary_or_hash)
      if hash?(ary_or_hash)
        init_with_hash(ary_or_hash)
      else
        init_with_ary(ary_or_hash)
      end
    end

    ##
    # The number of elements in the tuple.

    def size
      @tuple.size
    end

    ##
    # Accessor method for elements of the tuple.

    def [](k)
      @tuple[k]
    end

    ##
    # Fetches item +k+ from the tuple.

    def fetch(k)
      @tuple.fetch(k)
    end

    ##
    # Iterate through the tuple, yielding the index or key, and the
    # value, thus ensuring arrays are iterated similarly to hashes.

    def each # FIXME
      if Hash === @tuple
        @tuple.each { |key, value| yield(key, value) }
      else
        @tuple.each_with_index { |value, key| yield(key, value) }
      end
    end

    ##
    # Return the tuple itself

    def value
      @tuple
    end

    private

    def hash?(ary_or_hash)
      ary_or_hash.respond_to?(:keys)
    end

    ##
    # Munges +ary+ into a valid Tuple.

    def init_with_ary(ary)
      @tuple = Array.new(ary.size)
      @tuple.size.times do |i|
        @tuple[i] = ary[i]
      end
    end

    ##
    # Ensures +hash+ is a valid Tuple.

    def init_with_hash(hash)
      @tuple = Hash.new
      hash.each do |k, v|
        raise InvalidHashTupleKey unless String === k
        @tuple[k] = v
      end
    end

  end

  ##
  # Templates are used to match tuples in Rinda.
  #
  # The +tuple+ must be the same size as the template.
  # An element with a +nil+ value in a template acts
  # as a wildcard, matching any value in the corresponding position in the
  # tuple.
  #
  # Elements of the template match the +tuple+ if the are == or ===
  #
  #   Template.new([:foo, 5]).match   Tuple.new([:foo, 5]) # => true
  #   Template.new([:foo, nil]).match Tuple.new([:foo, 5]) # => true
  #   Template.new([String]).match    Tuple.new(['hello']) # => true
  #
  #   Template.new([:foo]).match      Tuple.new([:foo, 5]) # => false
  #   Template.new([:foo, 6]).match   Tuple.new([:foo, 5]) # => false
  #   Template.new([:foo, nil]).match Tuple.new([:foo])    # => false
  #   Template.new([:foo, 6]).match   Tuple.new([:foo])    # => false


  class Template < Tuple

    ##
    # Matches this template against +tuple+.

    def match(tuple)
      return false unless tuple.respond_to?(:size)
      return false unless tuple.respond_to?(:fetch)
      return false unless self.size == tuple.size
      each do |k, v|
        begin
          it = tuple.fetch(k)
        rescue
          return false
        end
        next if v.nil?
        next if v == it
        next if v === it
        return false
      end
      return true
    end

    ##
    # Alias for #match.

    def ===(tuple)
      match(tuple)
    end

  end

  ##
  # A DRb::DRbObject evaluates === as a match for both +uri+ and +ref+.
  #
  # Rinda::DRbObjectTemplate allows for a more specific comparison. You can
  # compare either by +uri+ or by +ref+ or considering both.
  #
  #   a = DRbObject.new(nil, "druby://host:1234")
  #   b = Rinda::DRbObjectTemplate.new
  #   a === b => True
  #
  #   a = DRbObject.new(nil, "druby://host:1234")
  #   b = Rinda::DRbObjectTemplate.new("druby://host:1234")
  #   a === b => True
  #
  #   a = DRbObject.new_with("druby://foo:12345", 1234)
  #   b = Rinda::DRbObjectTemplate.new(/^druby:\/\/(foo|bar):/)
  #   a === b => True

  class DRbObjectTemplate

    ##
    # Creates a new DRbObjectTemplate that will match against +uri+ and/or +ref+.

    def initialize(uri=nil, ref=nil)
      @drb_uri = uri
      @drb_ref = ref
    end

    ##
    # This DRbObjectTemplate matches +ro+ if the remote object's drburi and
    # drbref are the same.  +nil+ is used as a wildcard.

    def ===(ro)
      return true if super(ro)
      unless @drb_uri.nil?
        return false unless (@drb_uri === ro.__drburi rescue false)
      end
      unless @drb_ref.nil?
        return false unless (@drb_ref === ro.__drbref rescue false)
      end
      true
    end

  end

  ##
  # TupleSpaceProxy allows a remote Tuplespace to appear as local.

  class TupleSpaceProxy

    ##
    # Creates a new TupleSpaceProxy to wrap +ts+.

    def initialize(ts)
      @ts = ts
    end

    ##
    # Adds +tuple+ to the proxied TupleSpace.  See TupleSpace#write.

    def write(tuple, sec=nil)
      @ts.write(tuple, sec)
    end

    ##
    # Takes +tuple+ from the proxied TupleSpace.  See TupleSpace#take.

    def take(tuple, sec=nil, &block)
      port = []
      @ts.move(DRbObject.new(port), tuple, sec, &block)
      port[0]
    end

    ##
    # Reads +tuple+ from the proxied TupleSpace.  See TupleSpace#read.

    def read(tuple, sec=nil, &block)
      @ts.read(tuple, sec, &block)
    end

    ##
    # Reads all tuples matching +tuple+ from the proxied TupleSpace.  See
    # TupleSpace#read_all.

    def read_all(tuple)
      @ts.read_all(tuple)
    end

    ##
    # Registers for notifications of event +ev+ on the proxied TupleSpace.
    # See TupleSpace#notify

    def notify(ev, tuple, sec=nil)
      @ts.notify(ev, tuple, sec)
    end

  end

  ##
  # An SimpleRenewer allows a TupleSpace to check if a TupleEntry is still
  # alive.

  class SimpleRenewer

    include DRbUndumped

    ##
    # Creates a new SimpleRenewer that keeps an object alive for another +sec+
    # seconds.

    def initialize(sec=180)
      @sec = sec
    end

    ##
    # Called by the TupleSpace to check if the object is still alive.

    def renew
      @sec
    end
  end

end

