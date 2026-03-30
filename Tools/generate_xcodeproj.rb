#!/usr/bin/env ruby

require 'fileutils'
require 'xcodeproj'

ROOT = File.expand_path('..', __dir__)
PROJECT_PATH = File.join(ROOT, 'Cluster Watch.xcodeproj')

APP_SOURCES = [
  'Cluster Watch/ClusterWatchApp.swift',
  'Cluster Watch/UI/MenuBarRootView.swift',
  'Cluster Watch/UI/StateBadgeView.swift',
  'Cluster Watch/UI/WatchedJobsSectionView.swift',
  'Cluster Watch/UI/WatchedJobRowView.swift',
  'Cluster Watch/UI/ClusterStatusSectionView.swift',
  'Cluster Watch/UI/BrowseJobsSectionView.swift',
  'Cluster Watch/UI/SettingsView.swift',
  'Shared/Models/ClusterConfig.swift',
  'Shared/Models/JobModels.swift',
  'Shared/Models/Reachability.swift',
  'Shared/Services/SlurmParsing.swift',
  'Shared/Services/SlurmClient.swift',
  'Shared/Services/NotificationManager.swift',
  'Shared/Services/PersistenceStore.swift',
  'Shared/Services/JobFormatting.swift',
  'Shared/Services/PollingCoordinator.swift',
  'Shared/Services/JobStore.swift',
  'Shared/ViewModels/GroupedJobsViewModel.swift'
].freeze

TEST_SOURCES = [
  'Tests/ClusterWatchCoreTests/SlurmParsingTests.swift',
  'Tests/ClusterWatchCoreTests/GroupedJobsViewModelTests.swift',
  'Tests/ClusterWatchCoreTests/PersistenceStoreTests.swift',
  'Tests/ClusterWatchCoreTests/JobStoreTests.swift'
].freeze

def ensure_group(root_group, relative_path)
  current_group = root_group

  relative_path.split('/').each do |component|
    next_group = current_group.groups.find { |group| group.display_name == component || group.path == component }
    current_group = next_group || current_group.new_group(component, component)
  end

  current_group
end

def add_file_reference(project, relative_path)
  folder = File.dirname(relative_path)
  group = folder == '.' ? project.main_group : ensure_group(project.main_group, folder)
  existing_reference = group.files.find { |file| file.path == File.basename(relative_path) }
  existing_reference || group.new_file(File.basename(relative_path))
end

FileUtils.rm_rf(PROJECT_PATH)

project = Xcodeproj::Project.new(PROJECT_PATH)
project.root_object.attributes['LastUpgradeCheck'] = '1600'
project.root_object.attributes['ORGANIZATIONNAME'] = 'Kirill Dubovikov'

app_target = project.new_target(:application, 'Cluster Watch', :osx, '14.0')
test_target = project.new_target(:unit_test_bundle, 'Cluster WatchTests', :osx, '14.0')
test_target.add_dependency(app_target)

app_target.build_configurations.each do |config|
  config.build_settings['PRODUCT_BUNDLE_IDENTIFIER'] = 'com.kirilldubovikov.ClusterWatch'
  config.build_settings['PRODUCT_NAME'] = 'Cluster Watch'
  config.build_settings['PRODUCT_MODULE_NAME'] = 'ClusterWatchCore'
  config.build_settings['INFOPLIST_FILE'] = 'Cluster Watch/Info.plist'
  config.build_settings['GENERATE_INFOPLIST_FILE'] = 'NO'
  config.build_settings['SWIFT_VERSION'] = '5.0'
  config.build_settings['MACOSX_DEPLOYMENT_TARGET'] = '14.0'
  config.build_settings['CODE_SIGN_STYLE'] = 'Automatic'
  config.build_settings['LD_RUNPATH_SEARCH_PATHS'] = ['$(inherited)', '@executable_path/../Frameworks']
  config.build_settings['ENABLE_HARDENED_RUNTIME'] = 'NO'
end

test_target.build_configurations.each do |config|
  config.build_settings['PRODUCT_BUNDLE_IDENTIFIER'] = 'com.kirilldubovikov.ClusterWatchTests'
  config.build_settings['PRODUCT_NAME'] = 'Cluster WatchTests'
  config.build_settings['PRODUCT_MODULE_NAME'] = 'ClusterWatchTests'
  config.build_settings['GENERATE_INFOPLIST_FILE'] = 'YES'
  config.build_settings['SWIFT_VERSION'] = '5.0'
  config.build_settings['MACOSX_DEPLOYMENT_TARGET'] = '14.0'
  config.build_settings['TEST_HOST'] = '$(BUILT_PRODUCTS_DIR)/Cluster Watch.app/Contents/MacOS/Cluster Watch'
  config.build_settings['BUNDLE_LOADER'] = '$(TEST_HOST)'
  config.build_settings['CODE_SIGN_STYLE'] = 'Automatic'
end

add_file_reference(project, 'Cluster Watch/Info.plist')

APP_SOURCES.each do |source_path|
  file_reference = add_file_reference(project, source_path)
  app_target.add_file_references([file_reference])
end

TEST_SOURCES.each do |source_path|
  file_reference = add_file_reference(project, source_path)
  test_target.add_file_references([file_reference])
end

project.recreate_user_schemes
project.save

Xcodeproj::XCScheme.share_scheme(PROJECT_PATH, 'Cluster Watch')
Xcodeproj::XCScheme.share_scheme(PROJECT_PATH, 'Cluster WatchTests')

shared_scheme_path = File.join(PROJECT_PATH, 'xcshareddata', 'xcschemes', 'Cluster Watch.xcscheme')
testable_block = <<~XML.chomp
         <TestableReference
            skipped = "NO">
            <BuildableReference
               BuildableIdentifier = "primary"
               BlueprintIdentifier = "#{test_target.uuid}"
               BuildableName = "Cluster WatchTests.xctest"
               BlueprintName = "Cluster WatchTests"
               ReferencedContainer = "container:Cluster Watch.xcodeproj">
            </BuildableReference>
         </TestableReference>
XML

scheme_contents = File.read(shared_scheme_path)
scheme_contents.sub!(
  "<Testables>\n      </Testables>",
  "<Testables>\n#{testable_block}\n      </Testables>"
)
File.write(shared_scheme_path, scheme_contents)

FileUtils.rm_rf(File.join(PROJECT_PATH, 'xcuserdata'))
