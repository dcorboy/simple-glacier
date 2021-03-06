#!/usr/bin/ruby -w

require 'rubygems'
require 'optparse'
require 'pp'
require 'json'
require 'aws-sdk'

#TODO
# listings should include all vaults? (can't default the vault name, then -- or could use different command)
# sync a glacier listing with local receipts?
# option to 'retry' failed uploads
# retry n times on initial upload
# maybe collection should be a hash with fn/desc as key?
# report collection sizes in general list output?
# fix stdout vs stderr output
# the ReceiptsFileIO/datastore thing should be an object, with the hash as an instance var
# delete and upload commands should be passed the vault name
# "client" should be an aws wrapper object :-/
# output strings are confusing


#############
### MAIN
#############

class SimpleGlacier
  def self.run
    # parse arguments and options
    begin
      $options = Parser.parse(ARGV)
    rescue OptionParser::InvalidOption => ex
      puts "Error: #{ex.message}"
      show_help_and_exit()
    end
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
    when 'inventory'
      cmd_obj = InventoryJob.new(ARGV, receipts)
    when 'jobs'
      cmd_obj = CheckJobs.new(ARGV, receipts)
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

Options = Struct.new(:receipts_file, :upload_name, :dry_run, :vault, :force, :debug)

class Parser
  def self.parse(options)
    args = Options.new("glacier_receipts.json", nil, false, "corbuntu_archive", false, false)

    opt_parser = OptionParser.new do |opts|
      opts.banner = "Usage: #{$0} [options] command [files]"

      opts.on("-n NAME", "--collection_name=NAME", "Name the upload collection for later reference") do |o|
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

      opts.on("-f", "--force", "Unless flag is present, archives with a Glacier archive ID will not be re-uploaded") do |o|
        args.force = o
      end

      opts.on("-t", "--test_debug", "If flag is present, AWS will not be called but otherwise actions will work normally") do |o|
        args.debug = o
      end

      opts.on("-h", "--help", "Prints this help") do
        puts opts
        printf("\ncommmands:\n")
        printf("%10s %-16s  --  %s\n", "upload", "file [file..]", "Upload files as a named collection, appending to an existing collection")
        printf("%10s %-16s  --  %s\n", "list", "", "List file information for a named collection, or list all collections")
        printf("%10s %-16s  --  %s\n", "delete", "", "Delete all Glacier archive files in named collection")
        printf("%10s %-16s  --  %s\n", "inventory", "", "Request an async Glacier inventory job for a given vault")
        printf("%10s %-16s  --  %s\n", "jobs", "", "Check completion of async Glacier jobs and write available results to an output file")
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
  @@version = 2

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
      {"version" => @@version}  # will create a new receipts file
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
    collection = receipts_object.deep_seek("vaults", vault_name, "collections", collection_name)
    if collection.nil? && create
      collection = receipts_object.deep_set([], "vaults", vault_name, "collections", collection_name)
    end
    collection
  end

  def self.get_vault_collections(receipts_object, vault_name, create = false)
    collection = receipts_object.deep_seek("vaults", vault_name, "collections")
    if collection.nil? && create
      collection = receipts_object.deep_set({}, "vaults", vault_name, "collections")
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

  def self.get_vault_jobs(receipts_object, vault_name, create = false)
    collection = receipts_object.deep_seek("vaults", vault_name, "pending_jobs")
    if collection.nil? && create
      collection = receipts_object.deep_set([], "vaults", vault_name, "pending_jobs")
    end
    collection
  end

  def self.update_version(json)
    from_version = json["version"] || 0
    if from_version < @@version
      puts "Converting receipts file from version: #{from_version}"
    elsif from_version > @@version
      puts "Incompatible receipts file version: #{from_version}"
      return nil
    end

    if from_version < 1
      json = {
        "vaults" => {
          $options.vault => json
        }
      }
    end

    if from_version < 2 # (convert from 1 -> 2) end ... etc
      vaults = json["vaults"]
      vaults.each do |vault, contents|
        vaults[vault] = {
          "collections" => contents
        }
      end
    end

    json["version"] = @@version
    # pp json
    json
  end
end

def shorten(id)
  id[0..16] + "..."
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

class MockInventoryResponse  # used during testing
  def initialize
    @job_id = "MOCKnTnEPDwwTDuivbmS-FvTTG3V3MlZIDnoYcTMH4xzu24iNkee67b8moEVALiLkfWuUN_og6JzgjfkMCdyylaWrg"
    @location = "MOCK/31545654644/vaults/corbuntu_archive/archives/nTnEmDwhuwTDuivb_ogKy8DcQzgjfkMCdyylaWrg"
  end

  attr_reader :job_id
  attr_reader :location

  def successful?
    true           # change for testing
  end
end

class MockDescribeJobResponse  # used during testing
  def initialize(succeed)
    @completed = succeed
    @status_code = succeed ? "Succeeded" : "InProgress"
  end

  attr_reader :completed
  attr_reader :status_code
end

class MockGetJobOutputResponse  # used during testing
  def initialize(succeed)
    @succeed = succeed
    @status = succeed ? 200 : 404
    @output = "VaultARN:arn:aws:glacier:us-east-1:923154980164:vaults/corbuntu_archive,InventoryDate:2015-10-12T13:46:09Z,ArchiveList:[{ArchiveId:EymLlqSjsnNKjTMrdplo734GhEe5lwRGbRGRqsJjDKiv6I8nFAycqjZKv5c4kpzpIuKsXzM4b59cssA6tN8WHa50vBzgkiX2p7o7wLZoEv01tBv7LGXitKaI2f5yus3Cw1q4fcAMJA"
  end

  attr_reader :status
  attr_reader :output

  def successful?
    @succeed
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

  def upload_glacier_archive(filename, collection)
    begin
      fileio = File.open(filename, 'r')
    rescue
      return puts "  Failed to open #{filename}"
    end

    if (receipt = collection.select{|archive| archive["filename"] == filename}[0])
      existing_id = receipt.deep_seek("glacier_response", "archive_id")
      if !$options.force && existing_id && !existing_id.empty?
        puts "  Skipping #{filename} -- A Glacier archive ID already exists and would be lost"
        puts "  Use -force option to allow the existing record to be overwritten"
        return false
      end
      puts "  Updating existing receipt for #{filename}"
      receipt["error"] = nil
      description = receipt["description"]
    else
      description = $options.upload_name + "::" + filename
      receipt = {
        "filename" => filename,
        "description" => description
      }
      collection << receipt
    end
    receipt["size"] = File.size(filename)
    glacier_response = upload_archive(fileio, description)
    success = $dry_run ? true : process_response(glacier_response, receipt)
    receipt["completed"] = Time.new
    success
  end

  protected
  def upload_archive(data, description)
    args = {
      account_id: "-",
      vault_name: $options.vault,
      archive_description: description,
      # checksum: "string",  # turns out, not actually needed
      body: data
    }

    if $dry_run
      puts "  Call client.upload_archive"
      puts "  with argument:"
      pp args
      [true, @@mock_response]
    else
      begin
        if $options.debug
          puts "  DEBUG: AWS upload was not called"
          # raise                    # testing return conditions
          [true, @@mock_response]
          # nil
        else
          [true, $client.upload_archive(args)]
        end
      rescue Exception => ex
        [false, ExceptionResponse.new(ex.class, ex.message)]
      end
    end
  end

  def process_response(response_pkg, receipt)
    unless response_pkg
      no_response = "nil response received from Glacier"
      puts "  ERROR -- #{no_response}"
      receipt["error"] = no_response
      receipt["glacier_response"] = {
        "glacier_error" => no_response,
        "glacier_message" => ""
      }
      return false
    end

    valid, response = response_pkg
    if valid
      if response.successful?
        receipt["glacier_response"] = {
          "archive_id" => response.archive_id,
          "checksum" => response.checksum,
          "location" => response.location
        }
        puts "  Glacier archive ID: #{response.archive_id}"
      else
        puts "  ERROR of type #{response.error.code} occurred"
        puts "  Error message is: #{response.error.message}"
        pp response # I have no idea what should be in here, but let's look at the whole thing

        receipt["error"] = "Something bad happened, but it wasn't an exception"
        receipt["glacier_response"] = {
          "glacier_error" => response.error.code,
          "glacier_message" => response.error.message
        }
      end
      response.successful?
    else
      puts "  ERROR of exception type #{response.ex_class} occurred"
      puts "  Exception message is: #{response.ex_message}"
      receipt["error"] = "Exception caught during upload"
      receipt["glacier_response"] = {
        "glacier_error" => response.ex_class,
        "glacier_message" => response.ex_message
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
    true
  end

  def banner_start
    puts "Really, the base class has no idea what you are doing, so don't ask it."
  end

  def do_action
    puts "The base class takes no action. You must at least override this method."
  end

  def banner_end
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
      if @@uploader.upload_glacier_archive(file, upload_receipts)
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

end

class List < GlacierCommand
  def initialize(argv, receipts)
    super(argv, receipts)
    @completed = 0
    @failed = 0
    @size = 0
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
          printf("  Description: %s\n", receipt["description"])
          glacier_response = receipt["glacier_response"]
          printf("  %s bytes\n", receipt["size"] || "unknown")
          if glacier_response && glacier_response["archive_id"]
            printf("  Archive file uploaded %s\n", receipt["completed"])
            printf("  Glacier archive ID: %s\n", shorten(glacier_response["archive_id"]))
            @completed += 1
            @size += receipt["size"] || 0
          else
            printf("  FAILED archive file upload at %s\n", receipt["completed"])
            printf("  Error message: %s\n", receipt["error"] ||= "None")
            @failed += 1
          end
        end
      end
    else
      if (@collection = ReceiptFileIO.get_vault_collections(@receipts, $options.vault)).nil?
        puts "No collections found for vault #{$options.vault}"
      else
        @collection.each do |name, archives|
          printf("  Collection: %-24s -- %d archives\n", name, archives.count)
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
        printf("\nUpload collection %s contains %d archives\n", $options.upload_name, @collection.count)
        puts "#{@completed} files complete, #{@failed} files failed to upload"
        puts "#{@size} bytes succesfully uploaded"
      else
        printf("\n%d collections, %d files total\n", @collection.count, @completed)
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
    puts "Successful deletion of #{receipt["filename"]} (#{receipt["description"]})"
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
        puts "  Call client.delete_archive"
        puts "  with argument:"
        pp args
        false  # change for testing
      else
        begin
          if $options.debug
            puts "  DEBUG: AWS delete was not called"
          else
            $client.delete_archive(args)
          end
          true
        rescue Exception => ex
          puts "  Delete failed for #{receipt["filename"]} (#{receipt["description"]}) -- An error of type #{ex.class} occurred"
          puts "  Glacier message is: #{ex.message}"
        end
      end
    else
      puts "  Removing #{receipt["filename"]} (#{receipt["description"]}) -- no Glacier archive ID"
      true
    end
  end
end

class InventoryJob < GlacierCommand
  @@job_type = "inventory-retrieval"
  @@mock_response = MockInventoryResponse.new

  def initialize(argv, receipts)
    super(argv, receipts)
    @succeeded = false
  end

  def check_args
    if @argv.length > 0
      puts "Glacier inventory job takes no arguments"
    else
      if $options.upload_name
        puts "Collection name ignored. Glacier inventory includes the entire vault"
      end
      true
    end
  end

  def banner_start
    puts "Inventory job request for #{$options.vault} sent at #{Time.new}"
  end

  def do_action
    jobs = ReceiptFileIO.get_vault_jobs(@receipts, $options.vault, true)
    if (job = initiate_glacier_inventory($options.vault, jobs))
      puts "Inventory job request for #{$options.vault} succeeded"
      puts "Glacier job ID #{shorten(job["job_id"])}"
      @succeeded = true
    else
      puts "Vault inventory request for #{$options.vault} FAILED"
    end
  end

  def save_receipts?
    @succeeded
  end

  #### protected class methods
  protected

  def initiate_glacier_inventory(vault, jobs)
    if (response = client_initiate_inventory(vault)) && response.successful?
      job = {
        "type" => @@job_type,
        "requested" => Time.new,
        "job_id" => response.job_id,
        "location" => response.location
      }
      jobs << job
      job
    end
  end

  def client_initiate_inventory(vault)
    args = {
      account_id: "-",
      vault_name: vault,
      job_parameters: { type: @@job_type }
    }
    if $dry_run
      puts "  Call client.initiate_job"
      puts "  with argument:"
      pp args
      # nil  # change for testing
      @@mock_response
    else
      begin
        if $options.debug
          puts "  DEBUG: AWS initiate_job was not called"
          @@mock_response
        else
          $client.initiate_job(args)
        end
      rescue Exception => ex
        puts "  Inventory job request failed for vault #{vault} -- An error of type #{ex.class} occurred"
        puts "  Glacier message is: #{ex.message}"
      end
    end
  end
end

class CheckJobs < GlacierCommand
  @@mock_describe_response = MockDescribeJobResponse.new(true)
  @@mock_output_response = MockGetJobOutputResponse.new(true)

  def initialize(argv, receipts)
    super(argv, receipts)
  end

  def check_args
    if @argv.length > 0
      puts "Checking Glacier jobs takes no arguments"
    else
      if $options.upload_name
        puts "Collection name ignored. Glacier jobs are not collection-specific"
      end
      true
    end
  end

  def banner_start
    puts "Retrieving Glacier jobs for #{$options.vault}"
  end

  def do_action
    jobs = ReceiptFileIO.get_vault_jobs(@receipts, $options.vault)
    if jobs && !jobs.empty?
      jobs.each do |job|
        completed, code = check_glacier_job(job, $options.vault)
        unless completed.nil?
          puts "Job #{job["type"]} status request for #{shorten(job["job_id"])} succeeded"
          puts "  Job status is #{code}"
          if completed
            output_file = "job_output." + job["job_id"][0..7]
            succeeded, code = get_glacier_job_output(job["job_id"], $options.vault, output_file)
            if succeeded
              puts "Job #{job["type"]} output request for #{shorten(job["job_id"])} succeeded"
              puts "  Job output sent to #{output_file}"
            else
              puts "Job #{job["type"]} status request for #{shorten(job["job_id"])} FAILED with code #{code}" if code
            end
          end
        end
      end
    else
      puts "No pending Glacier jobs for vault #{$options.vault}"
    end
  end

  def save_receipts?
    false # we do not change the pending jobs data for now
  end

  #### protected class methods
  protected

  def check_glacier_job(job, vault)
    if (response = client_describe_job(job["job_id"], vault))
      [response.completed, response.status_code]
    end
  end

  def client_describe_job(job_id, vault)
    args = {
      account_id: "-",
      vault_name: vault,
      job_id: job_id
    }
    if $dry_run
      puts "  Call client.describe_job"
      puts "  with argument:"
      pp args
      # nil  # change for testing
      @@mock_describe_response
    else
      begin
        if $options.debug
          puts "  DEBUG: AWS describe_job was not called"
          @@mock_describe_response
          # raise
        else
          $client.describe_job(args)
        end
      rescue Exception => ex
        puts "Job status request for #{shorten(job_id)} FAILED"
        puts "  An error of type #{ex.class} occurred"
        puts "  Glacier message is: #{ex.message}"
      end
    end
  end

  def get_glacier_job_output(job_id, vault, output_file)
    (response = client_get_job_output(job_id, vault, output_file)) ? [response.successful?, response.status] : [false, nil]
  end

  def client_get_job_output(job_id, vault, output_file)
    args = {
      account_id: "-",
      vault_name: vault,
      job_id: job_id,
      response_target: output_file
    }
    if $dry_run
      puts "  Call client.get_job_output"
      puts "  with argument:"
      pp args
      # nil  # change for testing
      @@mock_output_response
    else
      begin
        if $options.debug
          puts "  DEBUG: AWS get_job_output was not called"
          # @@mock_output_response.output --> output_file
          @@mock_output_response
          # raise
        else
          resp = $client.get_job_output(args)
        end
      rescue Exception => ex
        puts "  Request for job output failed for job ID #{shorten(job_id)} -- An error of type #{ex.class} occurred"
        puts "  Glacier message is: #{ex.message}"
      end
    end
  end
end

###############
### SCRIPT BODY
###############

SimpleGlacier.run
