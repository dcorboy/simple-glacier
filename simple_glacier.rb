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

class ReceiptFileIO
  def self.load_receipts(filename)
    if File.file?(filename)
      file = File.read(filename)
      begin
        JSON.parse(file)
      rescue
        nil
      end
    else
      {}  # will create a new receipts file
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

  def self.get_collection_receipts(receipts_object, upload_name)
    unless (upload = receipts_object[upload_name])
      upload = (receipts_object[upload_name] = [])
    end
    upload
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
      @@mock_response
    else
      $client.upload_archive(args)
      # @@mock_response
    end
  end

  def process_glacier_response(response, receipt)
    if response.successful?
      receipt[:glacier_response] = {
        :archive_id => response.archive_id,
        :checksum => response.checksum,
        :location => response.location
      }
    else
      puts response.error
      pp response # I have no idea what should be in here

      resp_error = ''  # why do I have to do this?
      resp_full = ''
      PP.pp(response.error, resp_error)
      PP.pp(response, resp_full)
      receipt[:error] = "Something bad happened"
      receipt[:glacier_response] = {
        :glacier_error => resp_error,
        :glacier_response => resp_full
      }
    end
    response.successful?
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
    upload_receipts = ReceiptFileIO.get_collection_receipts(@receipts, $options.upload_name)
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
    unless (glacier_response = @@uploader.upload_archive(fileio, filename))
      return puts "AWS Glacier upload for #{filename} failed with no response"
    end
    success = $dry_run ? true : @@uploader.process_glacier_response(glacier_response, receipt)
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
      @upload_receipts = ReceiptFileIO.get_collection_receipts(@receipts, $options.upload_name)
      @upload_receipts.each do |receipt|
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
    else
      @receipts.each do |name, archives|
        printf("Collection: %-24s -- %d archives\n", name, archives.count)
        @completed += archives.count
      end
    end
  end

  def banner_end
    if $options.upload_name
      puts "Upload collection #{$options.upload_name} contains #{@upload_receipts.count} archives"
      puts "#{@completed} files complete, #{@failed} files failed to upload"
    else
      puts "#{@receipts.count} collections, #{@completed} files total"
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
    if (upload_receipts = ReceiptFileIO.get_collection_receipts(@receipts, $options.upload_name)).empty?
      puts "No archive files found for collection #{$options.upload_name}"
    else
      upload_receipts.delete_if do |receipt|
        (success = delete_glacier_archive(receipt, upload_receipts)) ? @completed += 1 : @failed += 1
        success
      end
      if upload_receipts.empty?
        @receipts.delete($options.upload_name)
      end
    end
  end

  def banner_end
    puts "Delete #{$options.upload_name} completed at #{Time.new}"
    puts "Archives deleted: #{@completed}, failed: #{@failed}"
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
          $client.delete_archive(args)
          # true
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
