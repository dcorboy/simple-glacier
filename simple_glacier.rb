#!/usr/bin/ruby -w

require 'rubygems'
require 'optparse'
require 'pp'
require 'json'
require 'aws-sdk'

#TODO
# option to overwrite existing named upload collection (default is append)
# json struct could use a header (version, failure metadata for retries?, vault collections, pending jobs)
# incorporate vault collections?
# add command to perform Glacier listing - job start, cache job id, check pending job, etc.
# sync a glacier listing with local receipts?
# option to 'retry' failed uploads
# retry n times on initial upload
# a manual re-upload shoudl replace the existing node (would fn comparisons ignore paths?)
# maybe collection should be a hash with fn/desc as key? (no, paths would f that up)
# record and report archive and collection sizes

#############
### MAIN
#############

class SimpleGlacier
  def self.run
    # parse arguments and options
    $options = Parser.parse(ARGV)
    # pp $options

    if ARGV.length == 0
      show_help_and_exit()
    end

    $dry_run = $options.dry_run  # some functions will behave differently

    # try to load the receipts file

    unless (receipts = ReceiptFileIO.load_receipts($options.receipts_file))
      $stderr.puts "Receipts JSON file #{$options.receipts_file} is not valid."
      exit 1
    end

    # looks like we are good to go, now what are we doing?

    command = ARGV.shift

    case command
    when 'upload'
      cmd_obj = Upload.new(ARGV, receipts)
    when 'list'
      cmd_obj = List.new(ARGV, receipts)
    when 'delete'
      cmd_obj = Delete.new(ARGV, receipts)
    else
      puts "Unknown command #{command}"
      show_help_and_exit()
    end

    # check command args
    show_help_and_exit() unless cmd_obj.check_args()

    # start 'em up
    cmd_obj.banner_start()

    # remind dry-run or create AWS Glacier client
    if ($dry_run)
      puts "Dry-run -- no actions will be taken"
    else
      $client = Aws::Glacier::Client.new
    end

    # action the actionable
    cmd_obj.do_action()

    # write out the modified receipts file, if needed by the command
    if !$dry_run && cmd_obj.save_receipts?
      ReceiptFileIO.save_receipts($options.receipts_file, receipts)
    end

    cmd_obj.banner_end()
  end
end


###################
### UTILITY CLASSES
###################

Options = Struct.new(:receipts_file, :upload_name, :dry_run, :vault)

class Parser
  def self.parse(options)
    args = Options.new("glacier_receipts.json", nil, false, "corbuntu_archive")

    opt_parser = OptionParser.new do |opts|
      opts.banner = "Usage: #{$0} [options] command [files]"

      opts.on("-n NAME", "--name_upload=NAME", "Name the upload collection for later reference") do |o|
        args.upload_name = o
      end

      opts.on("-r FILE", "--receipts_file=FILE", "JSON archive of upload-receipts") do |o|
        args.receipts_file = o
      end

      opts.on("-v VAULT", "--vault_name=VAULT", "Glacier vault name") do |o|
        args.vault = o
      end

      opts.on("-d", "--dry_run", "If flag is present, no actions are taken and are instead displayed") do |o|
        args.dry_run = o
      end

      opts.on("-h", "--help", "Prints this help") do
        puts opts
        printf("\ncommmands:\n")
        printf("%10s %-16s  --  %s\n", "upload", "file [file..]", "Upload files as a named collection, appending to an existing collection")
        printf("%10s %-16s  --  %s\n", "list", "", "List file information for a named collection, or list all collections")
        printf("%10s %-16s  --  %s\n", "delete", "", "Delete all Glacier archive files in named collection")
        exit
      end
    end

    opt_parser.parse!(options)
    return args
  end
end

def show_help_and_exit()
  pp Parser.parse %w[--help]  # exits
end

class Hash
  def deep_seek(*_keys_)
    last_level    = self
    sought_value  = nil

    _keys_.each_with_index do |_key_, _idx_|
      if last_level.is_a?(Hash) && last_level.has_key?(_key_)
        if _idx_ + 1 == _keys_.length
          sought_value = last_level[_key_]
        else
          last_level = last_level[_key_]
        end
      else
        break
      end
    end

    sought_value
  end

  def deep_set(value, *_keys_)
    level    = self

    _keys_.each_with_index do |_key_, _idx_|
      if _idx_ + 1 == _keys_.length
        level[_key_] = value
      elsif level.has_key?(_key_)
        level = level[_key_]
      else
        level = (level[_key_] = {})
      end
    end
    value
  end
end

class ReceiptFileIO
  def self.load_receipts(filename)
    if File.file?(filename)
      file = File.read(filename)
      begin
        json = JSON.parse(file)
      rescue
        nil
      end
      update_version(json)
    else
      {"version" => 1}  # will create a new receipts file
    end
  end

  def self.save_receipts(filename, receipts_hash)
    unless ($dry_run)
      file = File.open(filename, 'w')
      file.write(JSON.pretty_generate(receipts_hash))
      file.write "\n"
      file.close
    end
  end

  def self.get_named_collection(receipts_object, vault_name, collection_name, create = false)
    collection = receipts_object.deep_seek("vaults", vault_name, collection_name)
    if collection.nil? && create
      collection = receipts_object.deep_set([], "vaults", vault_name, collection_name)
    end
    collection
  end

  def self.get_vault_collections(receipts_object, vault_name, create = false)
    collection = receipts_object.deep_seek("vaults", vault_name)
    if collection.nil? && create
      collection = receipts_object.deep_set({}, "vaults", vault_name)
    end
    collection
  end

  def self.get_vaults(receipts_object, create = false)
    collection = receipts_object["vaults"]
    if collection.nil? && create
      collection = (receipts_object["vaults"] = {})
    end
    collection
  end

  def self.update_version(json)
    from_version = json["version"] || 0
    puts "Source version: #{from_version}"

    if from_version < 1
      puts "version 0"
      json = {
        "version" => 1,
        "vaults" => {
          $options.vault => json
        }
      }
    end
    # if from_version < 2 (convert from 1 -> 2) end ... etc

    json
  end
end


###########################
### COMMAND UTILITY CLASSES
###########################

class MockUploadResponse  # used during testing
  def initialize
    @archive_id = "MOCKnTnEPDwwTDuivbmS-FvTTG3V3MlZIDnoYcTMH4xzu24iNkee67b8moEVALiLkfWuUN_og6JzgjfkMCdyylaWrg"
    @checksum = "MOCK3a35367088c595b367a30eb334942412584dbecaff542dd2376d25685cdd8662"
    @location = "MOCK/31545654644/vaults/corbuntu_archive/archives/nTnEmDwhuwTDuivb_ogKy8DcQzgjfkMCdyylaWrg"
    @error = "I have no idea what the error might be"
  end

  attr_reader :archive_id
  attr_reader :checksum
  attr_reader :location
  attr_reader :error

  def successful?
    true           # change for testing
  end
end

class ExceptionResponse
  def initialize(ex_class, ex_message)
    @ex_class = ex_class
    @ex_message = ex_message
  end

  attr_reader :ex_class
  attr_reader :ex_message
end

class GlacierUploaderCore
  @@mock_response = MockUploadResponse.new

  def upload_archive(data, description)
    args = {
      account_id: "-",
      vault_name: $options.vault,
      archive_description: description,
      # checksum: "string",  # turns out, not actually needed
      body: data
    }

    if $dry_run
      puts "Call client.upload_archive"
      puts "with argument:"
      pp args
      [true, @@mock_response]
    else
      begin
        # [true, $client.upload_archive(args)]
        # raise                    # testing return conditions
        [true, @@mock_response]
        # nil
      rescue Exception => ex
        [false, ExceptionResponse.new(ex.class, ex.message)]
      end
    end
  end

  def process_response(response_pkg, receipt)
    unless response_pkg
      no_response = "nil response received from Glacier"
      puts "ERROR #{no_response}"
      receipt[:error] = no_response
      receipt[:glacier_response] = {
        :glacier_error => no_response,
        :glacier_message => ""
      }
      return false
    end

    valid, response = response_pkg
    if valid
      if response.successful?
        receipt[:glacier_response] = {
          :archive_id => response.archive_id,
          :checksum => response.checksum,
          :location => response.location
        }
      else
        puts "ERROR of type #{response.error.code} occurred"
        puts "Error message is: #{response.error.message}"
        pp response # I have no idea what should be in here, but let's look at the whole thing

        receipt[:error] = "Something bad happened, but it wasn't an exception"
        receipt[:glacier_response] = {
          :glacier_error => response.error.code,
          :glacier_message => response.error.message
        }
      end
      response.successful?
    else
      puts "ERROR of exception type #{response.ex_class} occurred"
      puts "Exception message is: #{response.ex_message}"
      receipt[:error] = "Exception caught during upload"
      receipt[:glacier_response] = {
        :glacier_error => response.ex_class,
        :glacier_message => response.ex_message
      }
      false
    end
  end
end

###################
### COMMAND CLASSES
###################

class GlacierCommand
  @@uploader = GlacierUploaderCore.new

  def initialize(argv, receipts)
    @argv = argv
    @receipts = receipts
  end

  def check_args
    puts "Please do not call the base class. This ends here."
  end

  def banner_start
    puts "An ill-advised call to the base class started at #{Time.new}"
  end

  def do_action
    puts "The base class takes no action. You should at least dynamically override this method."
  end

  def banner_end
    puts "The call to the base class that you made in error completed at #{Time.new}"
  end

  def save_receipts?
    false
  end
end

class Upload < GlacierCommand
  def initialize(argv, receipts)
    super(argv, receipts)
    @completed = 0
    @failed = 0
    $options.upload_name ||= rand(36**8).to_s(36)
  end

  def check_args
    if @argv.length > 0
      true
    else
      puts "No files specified for upload"
    end 
  end

  def banner_start
    puts "Upload #{$options.upload_name} started at #{Time.new}"
  end

  def do_action
    upload_receipts = ReceiptFileIO.get_named_collection(@receipts, $options.vault, $options.upload_name, true)
    @argv.each do |file|
      if upload_glacier_archive(file, upload_receipts)
        @completed += 1
        puts "Archive #{file} uploaded successfully at #{Time.new}"
      else
        @failed += 1
        puts "Archive #{file} FAILED upload at #{Time.new}"
      end
    end
  end

  def banner_end
    puts "Upload #{$options.upload_name} completed at #{Time.new}"
    puts "Archive transfers completed: #{@completed}, failed: #{@failed}"
  end

  def save_receipts?
    true
  end

  #### protected class methods
  protected

  def upload_glacier_archive(filename, collection)
    receipt = {
      :filename => filename,
      :description => filename
    }
    begin
      fileio = File.open(filename, 'r')
    rescue
      return puts "Failed to open #{filename}"
    end
    glacier_response = @@uploader.upload_archive(fileio, filename)
    success = $dry_run ? true : @@uploader.process_response(glacier_response, receipt)
    receipt[:completed] = Time.new
    collection << receipt
    success
  end
end

class List < GlacierCommand
  def initialize(argv, receipts)
    super(argv, receipts)
    @completed = 0
    @failed = 0
    @upload_receipts = nil
  end

  def check_args
    if @argv.length > 0
      puts "List takes no arguments. Specify collection name using '-n NAME' switch"
    else
      true
    end 
  end

  def banner_start
    if $options.upload_name
      puts "Local listing for collection #{$options.upload_name}:"
    else
      puts "Local listing of all collections"
    end
  end

  def do_action
    if $options.upload_name
      if (@collection = ReceiptFileIO.get_named_collection(@receipts, $options.vault, $options.upload_name)).nil?
        puts "No archive files found for collection #{$options.upload_name}"
      else
        @collection.each do |receipt|
          puts receipt["filename"]
          printf("Description: %s\n", receipt["description"])
          glacier_response = receipt["glacier_response"]
          if glacier_response && glacier_response["archive_id"]
            printf("Archive file uploaded %s\n", receipt["completed"])
            printf("Glacier archive ID: %s\n\n", glacier_response["archive_id"])
            @completed += 1
          else
            printf("FAILED archive file upload at %s\n", receipt["completed"])
            printf("Error message: %s\n\n", receipt["error"] ||= "None")
            @failed += 1
          end
        end
      end
    else
      if (@collection = ReceiptFileIO.get_vault_collections(@receipts, $options.vault)).nil?
        puts "No collections found for vault #{$options.vault}"
      else
        @collection.each do |name, archives|
          printf("Collection: %-24s -- %d archives\n", name, archives.count)
          @completed += archives.count
        end
      end
    end
  end

  def banner_end
    if @collection.nil?
      puts "Listing failed"
    else
      if $options.upload_name
        puts "Upload collection #{$options.upload_name} contains #{@collection.count} archives"
        puts "#{@completed} files complete, #{@failed} files failed to upload"
      else
        puts "#{@collection.count} collections, #{@completed} files total"
      end
    end
  end
end

class Delete < GlacierCommand
  def initialize(argv, receipts)
    super(argv, receipts)
    @completed = 0
    @failed = 0
  end

  def check_args
    if !$options.upload_name
      puts "Name of collection to delete must be specified using '-n NAME' switch" 
    elsif @argv.length > 0
      puts "Delete takes no arguments. Specify collection name using '-n NAME' switch"
    else
      true
    end
  end

  def banner_start
    puts "Deletion of upload collection #{$options.upload_name} started at #{Time.new}"
  end

  def do_action
    if (@collection = ReceiptFileIO.get_named_collection(@receipts, $options.vault, $options.upload_name)).nil?
      puts "No archive files found for collection #{$options.upload_name}"
    else
      @collection.delete_if do |receipt|
        (success = delete_glacier_archive(receipt, @collection)) ? @completed += 1 : @failed += 1
        success
      end
      if @collection.empty?
        ReceiptFileIO.get_vault_collections(@receipts, $options.vault).delete($options.upload_name)
      end
    end
  end

  def banner_end
    if @collection.nil?
      puts "Delete failed"
    else
      puts "Delete #{$options.upload_name} completed at #{Time.new}"
      puts "Archives deleted: #{@completed}, failed: #{@failed}"
    end
  end

  def save_receipts?
    true
  end

  #### protected class methods
  protected

  def delete_glacier_archive(receipt, collection)
    return unless client_delete_archive(receipt)
    puts "Successful deletetion of #{receipt["filename"]} (#{receipt["description"]})"
    true
  end

  def client_delete_archive(receipt)
    if (glacier_upload_response = receipt["glacier_response"]) && (id = glacier_upload_response["archive_id"])
      args = {
        account_id: "-",
        vault_name: $options.vault,
        archive_id: id
      }
      if $dry_run
        puts "Call client.delete_archive"
        puts "with argument:"
        pp args
        false  # change for testing
      else
        begin
          # $client.delete_archive(args)
          true
        rescue Exception => ex
          puts "Delete failed for #{receipt["filename"]} (#{receipt["description"]}) -- An error of type #{ex.class} occurred"
          puts "Glacier message is: #{ex.message}"
        end
      end
    else
      puts "Skipping #{receipt["filename"]} (#{receipt["description"]}) -- no Glacier archive ID"
    end
  end
end

###############
### SCRIPT BODY
###############

SimpleGlacier.run
