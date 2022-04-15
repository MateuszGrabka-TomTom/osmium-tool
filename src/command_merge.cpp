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
#include <osmium/osm/way.hpp>
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
    ("new-conflict-resolution-strategy,N", "Use new conflict resolution strategy")
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

    if (vm.count("new-conflict-resolution-strategy")) {
        m_use_new_conflict_resolution_strategy = true;
    }

    if (vm.count("conflicts-output")) {
        m_conflicts_output_file = vm["conflicts-output"].as<std::string>();
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

void CommandMerge::init_node_builder(osmium::builder::NodeBuilder& node_builder, const osmium::Node& node) {
    node_builder.set_id(node.id());
    node_builder.set_visible(node.visible());
    node_builder.set_version(node.version());
    node_builder.set_changeset(node.changeset());
    node_builder.set_uid(node.uid());
    node_builder.set_timestamp(node.timestamp());
    node_builder.set_location(node.location());
    node_builder.set_user(node.user());
}

void CommandMerge::init_way_builder(osmium::builder::WayBuilder& way_builder, const osmium::Way& way) {
    way_builder.set_id(way.id());
    way_builder.set_visible(way.visible());
    way_builder.set_version(way.version());
    way_builder.set_changeset(way.changeset());
    way_builder.set_uid(way.uid());
    way_builder.set_timestamp(way.timestamp());
    way_builder.set_user(way.user());
    way_builder.add_item(way.nodes());
}

void CommandMerge::init_relation_builder(osmium::builder::RelationBuilder& relation_builder, const osmium::Relation& relation) {
    relation_builder.set_id(relation.id());
    relation_builder.set_visible(relation.visible());
    relation_builder.set_version(relation.version());
    relation_builder.set_changeset(relation.changeset());
    relation_builder.set_uid(relation.uid());
    relation_builder.set_timestamp(relation.timestamp());
    relation_builder.set_user(relation.user());
    relation_builder.add_item(relation.members());
}

void CommandMerge::report_conflict_on_versions(std::vector<QueueElement>& duplicates, const std::string& type) {
    std::stringstream ss;
    const osmium::Node* node_0 = static_cast<const osmium::Node*>(&duplicates[0].object());

    for(std::size_t i = 1; i < duplicates.size(); ++i) {
        const osmium::Node* node_i = static_cast<const osmium::Node*>(&duplicates[i].object());
        if (node_0->version() != node_i->version()) {
            ss << "003;" << type << ";" << std::to_string(node_0->id())
               << ";" << std::to_string(duplicates[0].data_source_index())
               << ";" << std::to_string(duplicates[i].data_source_index())
               << ";" << std::to_string(node_0->version())
               << ";" << std::to_string(node_i->version()) << '\n';
        }
    }
    report_conflict(ss);
}

void CommandMerge::report_conflict_on_locations(std::vector<QueueElement>& duplicates) {
    std::stringstream ss;
    const osmium::Node* node_0 = static_cast<const osmium::Node*>(&duplicates[0].object());

    for(std::size_t i = 1; i < duplicates.size(); ++i) {
        const osmium::Node* node_i = static_cast<const osmium::Node*>(&duplicates[i].object());
        if (node_0->version() == node_i->version() && node_0->location() != node_i->location()) {
            ss << "001;n;" << std::to_string(node_0->id())
               << ";" << std::to_string(duplicates[0].data_source_index())
               << ";" << std::to_string(duplicates[i].data_source_index()) << '\n';
        }
    }
    report_conflict(ss);
}

bool are_equal(const osmium::WayNodeList& left, const osmium::WayNodeList& right) {
    if (left.size() != right.size()) {
        return false;
    }

    auto left_it = left.begin();
    auto right_it = right.begin();

    while(left_it != left.end()) {
        if (*left_it != *right_it) {
            return false;
        }

        left_it++;
        right_it++;
    }

    return true;
}

void CommandMerge::report_conflict_on_nodes_list(std::vector<QueueElement>& duplicates) {
    std::stringstream ss;

    const osmium::Way* way_0 = static_cast<const osmium::Way*>(&duplicates[0].object());

    for(std::size_t i = 1; i < duplicates.size(); ++i) {
        const osmium::Way* way_i = static_cast<const osmium::Way*>(&duplicates[i].object());
        if (way_0->version() == way_i->version() && !are_equal(way_0->nodes(), way_i->nodes())) {
            ss << "001;w;" << std::to_string(way_0->id())
               << ";" << std::to_string(duplicates[0].data_source_index())
               << ";" << std::to_string(duplicates[i].data_source_index()) << '\n';
        }
    }
    report_conflict(ss);
}

bool are_equal(const osmium::RelationMemberList& left, const osmium::RelationMemberList& right) {
    if (left.size() != right.size()) {
        return false;
    }

    auto left_it = left.begin();
    auto right_it = right.begin();

    while(left_it != left.end()) {
        auto& left_member = *left_it;
        auto& right_member = *right_it;

        if (!(left_member.ref() == right_member.ref() && left_member.type() == right_member.type() && std::strcmp(left_member.role(), right_member.role()) == 0)) {
            return false;
        }

        left_it++;
        right_it++;
    }

    return true;
}

void CommandMerge::report_conflict_on_members_list(std::vector<QueueElement>& duplicates) {
    std::stringstream ss;

    const osmium::Relation* relation_0 = static_cast<const osmium::Relation*>(&duplicates[0].object());

    for(std::size_t i = 1; i < duplicates.size(); ++i) {
        const osmium::Relation* relation_i = static_cast<const osmium::Relation*>(&duplicates[i].object());
        if (relation_0->version() == relation_i->version() && !are_equal(relation_0->members(), relation_i->members())) {
            ss << "001;r;" << std::to_string(relation_0->id())
               << ";" << std::to_string(duplicates[0].data_source_index())
               << ";" << std::to_string(duplicates[i].data_source_index()) << '\n';
        }
    }
    report_conflict(ss);

}

void CommandMerge::merge_tags(osmium::builder::TagListBuilder& builder, std::vector<QueueElement>& duplicates, const std::string& type) {
    std::map<std::string, std::string> merged_tags{};
    std::map<std::string, std::string>::iterator it;
    std::stringstream ss;
    for(std::size_t i = 0; i < duplicates.size(); ++i) {
        const osmium::TagList& tags = duplicates[i].object().tags();

        for (auto tag_it = tags.begin(); tag_it != tags.end(); ++tag_it) {
            const osmium::Tag& tag = *tag_it;

            std::string key = tag.key();
            it = merged_tags.find(key);

            if (it == merged_tags.end()) {
                merged_tags.insert({tag.key(), tag.value()});
            } else if (tag.value() != it->second) {
                ss << "002;" << type << ";" << std::to_string(duplicates[0].object().id())
                   << ";" << std::to_string(duplicates[0].data_source_index())
                   << ";" << std::to_string(duplicates[i].data_source_index())
                   << ";" << tag.key() << '\n';
            }
        }
    }
    report_conflict(ss);
    for (auto it = merged_tags.begin(); it != merged_tags.end(); it++) {
        builder.add_tag(it->first, it->second);
    }
}

void CommandMerge::add_tags(osmium::builder::TagListBuilder& builder, std::vector<QueueElement>& duplicates, const std::string& type) {
    merge_tags(builder, duplicates, type);
}

void CommandMerge::deduplicate_and_write(std::vector<QueueElement>& duplicates, osmium::io::Writer& writer) {
    if (duplicates.size() == 1) {
        writer(duplicates[0].object());
        return;
    }
    // sort by version
    std::sort(duplicates.begin(), duplicates.end(), [](const QueueElement& l, const QueueElement& r) -> bool
        {
            if (l.object().version() == r.object().version()) {
                return l.data_source_index() < r.data_source_index();
            }
            return l.object().version() > r.object().version();
        });

    // merge
    const osmium::OSMObject& object = duplicates.front().object();
    osmium::memory::Buffer buffer{1024, osmium::memory::Buffer::auto_grow::yes};
    switch(object.type()) {
        case osmium::item_type::node : {
            report_conflict_on_versions(duplicates, "n");
            report_conflict_on_locations(duplicates);
            {
                osmium::builder::NodeBuilder builder{buffer};
                init_node_builder(builder, static_cast<const osmium::Node&>(object));
                osmium::builder::TagListBuilder tagbuilder{builder};
                add_tags(tagbuilder, duplicates, "n");
            }
            writer(buffer.get<osmium::Node>(buffer.commit()));
            break;
        }
        case osmium::item_type::way : {
            report_conflict_on_versions(duplicates, "w");
            report_conflict_on_nodes_list(duplicates);
            {
                osmium::builder::WayBuilder builder{buffer};
                init_way_builder(builder, static_cast<const osmium::Way&>(object));
                osmium::builder::TagListBuilder tagbuilder{builder};
                add_tags(tagbuilder, duplicates, "n");
            }
            writer(buffer.get<osmium::Way>(buffer.commit()));
            break;
        }
        case osmium::item_type::relation : {
            report_conflict_on_versions(duplicates, "r");
            report_conflict_on_members_list(duplicates);
            {
                osmium::builder::RelationBuilder builder{buffer};
                init_relation_builder(builder, static_cast<const osmium::Relation&>(object));
                osmium::builder::TagListBuilder tagbuilder{builder};
                add_tags(tagbuilder, duplicates, "n");
            }
            writer(buffer.get<osmium::Relation>(buffer.commit()));
            break;
        }
        default : { // is this even possible?
            for(std::size_t i = 0; i < duplicates.size(); ++i) {
                writer(duplicates[i].object());
            }
            break;
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
        std::stringstream index_to_data_source_log;

        int index = 0;
        for (const osmium::io::File& file : m_input_files) {
            if (index != 0) {
                index_to_data_source_log << ";";
            }

            index_to_data_source_log << std::to_string(index) << ";" << file.filename();

            data_sources.emplace_back(file, m_with_history);

            if (!data_sources.back().empty()) {
                queue.emplace(data_sources.back().get(), index);
            }

            ++index;
        }
        index_to_data_source_log << '\n';

        if (m_use_new_conflict_resolution_strategy) {
            size_t m_conflicts_buf_size = 128UL * 1024UL * 1024UL; // 128MB buffer for conflict file-writes
            std::vector<char> vec(m_conflicts_buf_size);
            m_conflicts_output.rdbuf()->pubsetbuf(&vec.front(), m_conflicts_buf_size);
            m_conflicts_output.open(m_conflicts_output_file, std::ios_base::trunc | std::ios_base::out);
            report_conflict(index_to_data_source_log);
        }

        std::vector<QueueElement> duplicates;

        int n = 0;
        while (!queue.empty()) {
            const auto element = queue.top();
            queue.pop();

            const osmium::OSMObject& obj = element.object();
            duplicates.push_back(element);

            if (!m_use_new_conflict_resolution_strategy || queue.empty() || obj.id() != queue.top().object().id() || obj.type() != queue.top().object().type()) {
                deduplicate_and_write(duplicates, writer);

                for(const auto& duplicate : duplicates) {
                    const int index = duplicate.data_source_index();
                    if (data_sources[index].next()) {
                        queue.emplace(data_sources[index].get(), index);
                    }
                }

                duplicates.clear();
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
    if (m_use_new_conflict_resolution_strategy) {
        m_conflicts_output.flush();
        m_conflicts_output.close();
    }
    writer.close();

    show_memory_used();
    m_vout << "Done.\n";

    return true;
}
