/*

Osmium -- OpenStreetMap data manipulation command line tool
https://osmcode.org/osmium-tool/

Copyright (C) 2013-2022  Jochen Topf <jochen@topf.org>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

*/

#include "command_merge.hpp"

#include "exception.hpp"
#include "util.hpp"

#include <osmium/builder/osm_object_builder.hpp>
#include <osmium/io/file.hpp>
#include <osmium/io/header.hpp>
#include <osmium/io/input_iterator.hpp>
#include <osmium/io/output_iterator.hpp>
#include <osmium/io/reader.hpp>
#include <osmium/io/reader_with_progress_bar.hpp>
#include <osmium/io/writer.hpp>
#include <osmium/memory/buffer.hpp>
#include <osmium/osm/entity_bits.hpp>
#include <osmium/osm/object.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/util/verbose_output.hpp>

#include <boost/program_options.hpp>

#include <algorithm>
#include <memory>
#include <numeric>
#include <queue>
#include <string>
#include <utility>
#include <vector>

bool CommandMerge::setup(const std::vector<std::string>& arguments) {
    po::options_description opts_cmd{"COMMAND OPTIONS"};
    opts_cmd.add_options()
    ("with-history,H", "Do not warn about input files with multiple object versions")
    ("conflicts-output,C", po::value<std::string>(), "Report conflicts to the given output")
    ;

    po::options_description opts_common{add_common_options()};
    po::options_description opts_input{add_multiple_inputs_options()};
    po::options_description opts_output{add_output_options()};

    po::options_description hidden;
    hidden.add_options()
    ("input-filenames", po::value<std::vector<std::string>>(), "Input files")
    ;

    po::options_description desc;
    desc.add(opts_cmd).add(opts_common).add(opts_input).add(opts_output);

    po::options_description parsed_options;
    parsed_options.add(desc).add(hidden);

    po::positional_options_description positional;
    positional.add("input-filenames", -1);

    po::variables_map vm;
    po::store(po::command_line_parser(arguments).options(parsed_options).positional(positional).run(), vm);
    po::notify(vm);

    if (!setup_common(vm, desc)) {
        return false;
    }
    setup_progress(vm);
    setup_input_files(vm);
    setup_output_file(vm);

    if (vm.count("with-history")) {
        m_with_history = true;
    }
    
    if (vm.count("conflicts-output")) {
        m_conflicts_output = vm["conflicts-output"].as<std::string>();
    }
    
    return true;
}

void CommandMerge::show_arguments() {
    show_multiple_inputs_arguments(m_vout);
    show_output_arguments(m_vout);
}

namespace {

    class DataSource {

        using it_type = osmium::io::InputIterator<osmium::io::Reader, osmium::OSMObject>;

        std::unique_ptr<osmium::io::Reader> m_reader;
        std::string m_name;
        it_type m_iterator;

        osmium::item_type m_last_type = osmium::item_type::node;
        osmium::object_id_type m_last_id = 0;
        osmium::object_version_type m_last_version = 0;

        bool m_warning;

    public:

        explicit DataSource(const osmium::io::File& file, bool with_history) :
            m_reader(std::make_unique<osmium::io::Reader>(file)),
            m_name(file.filename()),
            m_iterator(*m_reader),
            m_warning(!with_history) {
            if (m_iterator != it_type{}) {
                m_last_type = m_iterator->type();
                m_last_id = m_iterator->id();
                m_last_version = m_iterator->version();
            }
        }

        bool empty() const noexcept {
            return m_iterator == it_type{};
        }

        bool next() {
            ++m_iterator;

            if (m_iterator == it_type{}) { // reached end of file
                return false;
            }

            if (m_iterator->type() < m_last_type) {
                throw std::runtime_error{"Objects in input file '" + m_name + "' out of order (must be nodes, then ways, then relations)."};
            }
            if (m_iterator->type() > m_last_type) {
                m_last_type = m_iterator->type();
                m_last_id = m_iterator->id();
                m_last_version = m_iterator->version();
                return true;
            }

            if (m_iterator->id() < m_last_id) {
                throw std::runtime_error{"Objects in input file '" + m_name + "' out of order (smaller ids must come first)."};
            }
            if (m_iterator->id() > m_last_id) {
                m_last_id = m_iterator->id();
                m_last_version = m_iterator->version();
                return true;
            }

            if (m_iterator->version() < m_last_version) {
                throw std::runtime_error{"Objects in input file '" + m_name + "' out of order (smaller version must come first)."};
            }
            if (m_iterator->version() == m_last_version) {
                throw std::runtime_error{"Two objects in input file '" + m_name + "' with same version."};
            }

            if (m_warning) {
                std::cerr << "Warning: Multiple objects with same id in input file '" + m_name + "'!\n";
                std::cerr << "If you are reading history files, this is to be expected. Use --with-history to disable warning.\n";
                m_warning = false;
            }

            m_last_version = m_iterator->version();

            return true;
        }

        const osmium::OSMObject* get() noexcept {
            return &*m_iterator;
        }

        std::size_t offset() const noexcept {
            return m_reader->offset();
        }

    }; // DataSource

} // anonymous namespace

void CommandMerge::init_builder(osmium::builder::NodeBuilder& node_builder, const osmium::OSMObject* first) {
    const osmium::Node* node = static_cast<const osmium::Node*>(first);

    node_builder.set_id(node->id());
    node_builder.set_visible(node->visible());
    node_builder.set_version(node->version());
    node_builder.set_changeset(node->changeset());
    node_builder.set_uid(node->uid());
    node_builder.set_timestamp(node->timestamp());
    node_builder.set_location(node->location());
    node_builder.set_user(node->user());
}

void CommandMerge::report_conflict_on_versions(std::vector<QueueElement>& duplicates) {
    const osmium::Node* node_0 = static_cast<const osmium::Node*>(&duplicates[0].object());

    for(std::size_t i = 1; i < duplicates.size(); ++i) {
        const osmium::Node* node_i = static_cast<const osmium::Node*>(&duplicates[i].object());  

        if (node_0->version() != node_i->version()) {
            std::string conflict_details = "0005;n;";
            conflict_details += std::to_string(node_0->id());
            conflict_details += ";";
            conflict_details += std::to_string(duplicates[i].data_source_index());
            conflict_details += ";";
            conflict_details += std::to_string(node_0->version());
            conflict_details += ";";
            conflict_details += std::to_string(duplicates[i].data_source_index());
            conflict_details += ";";
            conflict_details += std::to_string(node_i->version());
            report_conflict(conflict_details);
        } 
    }
}

void CommandMerge::report_conflict_on_locations(std::vector<QueueElement>& duplicates) {
    const osmium::Node* node_0 = static_cast<const osmium::Node*>(&duplicates[0].object());

    for(std::size_t i = 1; i < duplicates.size(); ++i) {
        const osmium::Node* node_i = static_cast<const osmium::Node*>(&duplicates[i].object());  

        if (node_0->version() == node_i->version() && node_0->location() != node_i->location()) {
            std::string conflict_details = "0001;n;";
            conflict_details += std::to_string(node_0->id());
            conflict_details += ";";
            conflict_details += std::to_string(duplicates[0].data_source_index());
            conflict_details += ";";
            conflict_details += std::to_string(duplicates[i].data_source_index());
            report_conflict(conflict_details);
        } 
    }
}

void CommandMerge::merge_tags(osmium::builder::NodeBuilder& node_builder, std::vector<QueueElement>& duplicates) {
    std::map<std::string, std::string> merged_tags{};
    std::map<std::string, std::string>::iterator it;

    for(std::size_t i = 0; i < duplicates.size(); ++i) {
        const osmium::TagList& tags = duplicates[i].object().tags();

        for (auto tag_it = tags.begin(); tag_it != tags.end(); ++tag_it) {
            const osmium::Tag& tag = *tag_it;

            std::string key = tag.key();
            it = merged_tags.find(key);
            
            if (it == merged_tags.end()) {
                merged_tags.insert({tag.key(), tag.value()});
            } else if (tag.value() != it->second) {
                std::string conflict_details = "0002;n;";
                conflict_details += std::to_string(duplicates[0].object().id());
                conflict_details += ";";
                conflict_details += std::to_string(duplicates[0].data_source_index());
                conflict_details += ";";
                conflict_details += tag.key();
                conflict_details += ";";
                conflict_details += std::to_string(duplicates[i].data_source_index());
                report_conflict(conflict_details);
            }
        }
    }

    
    osmium::builder::TagListBuilder builder{node_builder};

    for (it = merged_tags.begin(); it != merged_tags.end(); it++) {
        builder.add_tag(it->first, it->second);
    }
}

void CommandMerge::deduplicate_and_write(std::vector<QueueElement>& duplicates, osmium::io::Writer* writer) {
    // sort by version
    struct {
        bool operator()(QueueElement l, QueueElement r) const { return l.object().version() > r.object().version(); }
    } compare_versions;

    std::sort(duplicates.begin(), duplicates.end(), compare_versions);

    // merge
    const osmium::OSMObject* first = &duplicates.front().object();
    
    if (first->type() == osmium::item_type::node) {
        osmium::memory::Buffer buffer{1024, osmium::memory::Buffer::auto_grow::yes};
        osmium::builder::NodeBuilder node_builder{buffer};

        init_builder(node_builder, first);
        report_conflict_on_versions(duplicates);
        report_conflict_on_locations(duplicates);
        merge_tags(node_builder, duplicates);
        
        (*writer)(buffer.get<osmium::Node>(buffer.commit()));
    } else {
        for(std::size_t i = 0; i < duplicates.size(); ++i) {
            (*writer)(duplicates[i].object());
        }   
    }
}

bool CommandMerge::run() {
    m_vout << "Opening output file...\n";
    osmium::io::Header header;
    setup_header(header);

    osmium::io::Writer writer{m_output_file, header, m_output_overwrite, m_fsync};

    if (m_input_files.size() == 1) {
        m_vout << "Single input file. Copying to output file...\n";
        osmium::io::ReaderWithProgressBar reader{display_progress(), m_input_files[0]};
        while (osmium::memory::Buffer buffer = reader.read()) {
            writer(std::move(buffer));
        }
    } else {
        m_vout << "Merging " << m_input_files.size() << " input files to output file...\n";
        osmium::ProgressBar progress_bar{file_size_sum(m_input_files), display_progress()};
        std::vector<DataSource> data_sources;
        data_sources.reserve(m_input_files.size());

        std::priority_queue<QueueElement> queue;

        std::string index_to_data_source_log;

        int index = 0;
        for (const osmium::io::File& file : m_input_files) {
            if (index != 0) {
                index_to_data_source_log += ";";
            }

            index_to_data_source_log += std::to_string(index);
            index_to_data_source_log += ";";
            index_to_data_source_log += file.filename();

            data_sources.emplace_back(file, m_with_history);

            if (!data_sources.back().empty()) {
                queue.emplace(data_sources.back().get(), index);
            }

            ++index;
        }

        report_conflict(index_to_data_source_log);

        std::vector<QueueElement> duplicates;

        int n = 0;
        while (!queue.empty()) {
            const auto element = queue.top();
            queue.pop();

            const osmium::OSMObject& obj = element.object();

            if (!queue.empty()) { 
                const osmium::OSMObject& next_obj = queue.top().object();

                if (obj.id() == next_obj.id() && obj.type() == next_obj.type()) {
                    duplicates.push_back(element);
                } else {
                    if (!duplicates.empty()) {
                        duplicates.push_back(element);
                        deduplicate_and_write(duplicates, &writer);
                        
                        for(std::size_t i = 0; i < duplicates.size(); ++i) {
                            const int index = duplicates[i].data_source_index();
                            if (data_sources[index].next()) {
                                queue.emplace(data_sources[index].get(), index);
                            }
                        }

                        duplicates.clear();
                    } else {
                        writer(obj);

                        const int index = element.data_source_index();
                        if (data_sources[index].next()) {
                            queue.emplace(data_sources[index].get(), index);
                        }
                    }
                }
            } else {
                if (duplicates.empty()) {
                    writer(obj);

                    const int index = element.data_source_index();
                    if (data_sources[index].next()) {
                        queue.emplace(data_sources[index].get(), index);
                    }
                } else {
                    duplicates.push_back(element);
                    deduplicate_and_write(duplicates, &writer);

                    for(std::size_t i = 0; i < duplicates.size(); ++i) {
                        const int index = duplicates[i].data_source_index();
                        if (data_sources[index].next()) {
                            queue.emplace(data_sources[index].get(), index);
                        }
                    }

                    duplicates.clear();
                }
            }
        
            if (n++ > 10000) {
                n = 0;
                progress_bar.update(std::accumulate(data_sources.cbegin(), data_sources.cend(), static_cast<std::size_t>(0), [](std::size_t sum, const DataSource& source){
                    return sum + source.offset();
                }));
            }
        }
    }

    m_vout << "Closing output file...\n";
    writer.close();

    show_memory_used();
    m_vout << "Done.\n";

    return true;
}

