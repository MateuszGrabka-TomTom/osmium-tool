#ifndef COMMAND_MERGE_HPP
#define COMMAND_MERGE_HPP

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

#include "cmd.hpp" // IWYU pragma: export

#include <osmium/builder/osm_object_builder.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/io/writer.hpp>

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <stdio.h>

namespace {
    class QueueElement {

        const osmium::OSMObject* m_object;
        int m_data_source_index;

    public:

        QueueElement(const osmium::OSMObject* object, int data_source_index) noexcept :
            m_object(object),
            m_data_source_index(data_source_index) {
        }

        const osmium::OSMObject& object() const noexcept {
            return *m_object;
        }

        int data_source_index() const noexcept {
            return m_data_source_index;
        }

    }; // QueueElement

    bool operator<(const QueueElement& lhs, const QueueElement& rhs) noexcept {
        return lhs.object() > rhs.object();
    }

    bool operator==(const QueueElement& lhs, const QueueElement& rhs) noexcept {
        return lhs.object() == rhs.object();
    }

    bool operator!=(const QueueElement& lhs, const QueueElement& rhs) noexcept {
        return !(lhs == rhs);
    }
}

class CommandMerge : public CommandWithMultipleOSMInputs, public with_osm_output {

    bool m_with_history = false;
    std::string m_conflicts_output;
    bool m_conflicts_output_cleaned_up = false;


public:

    explicit CommandMerge(const CommandFactory& command_factory) :
        CommandWithMultipleOSMInputs(command_factory) {
    }

    bool setup(const std::vector<std::string>& arguments) override final;

    void show_arguments() override final;

    bool run() override final;

    const char* name() const noexcept override final {
        return "merge";
    }

    const char* synopsis() const noexcept override final {
        return "osmium merge [OPTIONS] OSM-FILE...";
    }

    void init_builder(osmium::builder::NodeBuilder& node_builder, const osmium::OSMObject* first);
    void report_conflict_on_versions(std::vector<QueueElement>& duplicates);
    void report_conflict_on_locations(std::vector<QueueElement>& duplicates);
    void merge_tags(osmium::builder::NodeBuilder& node_builder, std::vector<QueueElement>& duplicates);
    void deduplicate_and_write(std::vector<QueueElement>& duplicates, osmium::io::Writer* writer);

private:


    void report_conflict(std::string message) {
        if (!m_conflicts_output.empty() && !m_conflicts_output_cleaned_up) {
            std::ofstream log;
            log.open(m_conflicts_output, std::ios_base::trunc | std::ios_base::out);
            log.close();
            m_conflicts_output_cleaned_up = true;
        }

        if (!m_conflicts_output.empty()) {
            std::ofstream log(m_conflicts_output, std::ios_base::app | std::ios_base::out);
            log << message << std::endl;
        }
    }

}; // class CommandMerge


#endif // COMMAND_MERGE_HPP
